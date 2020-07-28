"""Records members' XP and level."""
import asyncio
import discord
from discord.ext.commands import guild_only
from .. import db
from ._utils import *
from dozer.bot import DOZER_LOGGER


class Levels(Cog):
    """Commands and event handlers for managing levels and XP."""

    def __init__(self, bot):
        super().__init__(bot)
        self._loop = bot.loop

        self._to_flush = []  # list (queue) of records to flush to the database
        self._last_flushed_at = self._loop.time()
        # == Values to tune for perf ==
        # If records haven't been flushed in the last N seconds, flush now
        self._flush_timeout = 60
        # If there are at least N records in queue, flush now
        self._flush_cap = 100

    async def _flush_records(self):
        # Grab the queue of records to flush, replacing it with an empty queue for the next iteration
        # If an exception is raised later in this method, this intentionally causes the queue to be discarded. The
        # exception was probably caused by some record in the queue being invalid. By discarding it, we ensure that
        # future iterations aren't also affected.
        records, self._to_flush = self._to_flush, []

        table = MemberXP.__tablename__
        stmt = f"""
                INSERT INTO {table} (guild_id, user_id, total_xp, last_given_at) VALUES ($1, $2, $3, $4)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET total_xp = {table}.total_xp + EXCLUDED.total_xp,
                last_given_at = EXCLUDED.last_given_at
                WHERE EXCLUDED.last_given_at - {table}.last_given_at >= 60;
                """
        async with db.Pool.acquire() as conn:
            await conn.executemany(stmt, records)

    @Cog.listener()
    async def on_message(self, message):
        if message.guild is None or message.author.bot:
            return
        now = self._loop.time()
        if len(self._to_flush) >= self._flush_cap or now > (self._last_flushed_at + self._flush_timeout):
            try:
                await self._flush_records()
            except Exception as e:
                DOZER_LOGGER.error("Error flushing XP data to database! %r", e)
            else:
                self._last_flushed_at = now
        self._to_flush.append((message.guild.id, message.author.id, 20, message.created_at.timestamp()))

    @command()
    @guild_only()
    async def rank(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        if self._to_flush:
            self._last_flushed_at = self._loop.time()
            await self._flush_records()

        records = await MemberXP.get_by(guild_id=ctx.guild.id, user_id=member.id)
        if not records:
            await ctx.send("That user isn't on the leaderboard.")
            return
        record = records[0]

        embed = discord.Embed(color=member.color, title=f"XP for {member.display_name}")
        embed.description = (f"Level TODO, TODO/TODO XP to level up ({record.total_xp} total)\n"
                             f"#TODO in this server")
        await ctx.send(embed=embed)


class MemberXP(db.DatabaseTable):
    """Holds the roles of those who leave"""
    __tablename__ = 'levels_member_xp'
    __uniques__ = 'guild__id, user_id'

    @classmethod
    async def initial_create(cls):
        """Create the table in the database"""
        async with db.Pool.acquire() as conn:
            await conn.execute(f"""
            CREATE TABLE {cls.__tablename__} (
            guild_id bigint NOT NULL,
            user_id bigint NOT NULL,
            total_xp int NOT NULL,
            last_given_at bigint NOT NULL, -- timestamp
            PRIMARY KEY (guild_id, user_id)
            )""")

    def __init__(self, guild_id, user_id, total_xp, last_given_at):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id
        self.total_xp = total_xp
        self.last_given_at = last_given_at

    @classmethod
    async def get_by(cls, **kwargs):
        results = await super().get_by(**kwargs)
        result_list = []
        for result in results:
            obj = MemberXP(guild_id=result.get("guild_id"), user_id=result.get("user_id"),
                           total_xp=result.get("total_xp"), last_given_at=result.get("last_given_at"))
            result_list.append(obj)
        return result_list


def setup(bot):
    """Add the levels cog to a bot."""
    bot.add_cog(Levels(bot))

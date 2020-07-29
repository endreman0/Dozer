"""Records members' XP and level."""
import asyncio

import asyncpg
import discord
import logging
from discord.ext.commands import guild_only
from discord.ext.tasks import loop

from ._utils import *
from .. import db

logger = logging.getLogger(__name__)


class Levels(Cog):
    """Commands and event handlers for managing levels and XP."""

    def __init__(self, bot):
        super().__init__(bot)
        self._loop = bot.loop
        self._records_to_flush = []

        self.flush_records.add_exception_type(asyncpg.PostgresError)
        self.flush_records.start()

    @loop(minutes=1)
    async def flush_records(self):
        if not self._records_to_flush:
            return  # nothing to do

        logger.debug('writing %s record(s) to the database', len(self._records_to_flush))

        # Grab the queue of records to flush, replacing it with an empty queue for the next iteration
        # If an exception is raised later in this method, this intentionally causes the queue to be discarded. The
        # exception was probably caused by some record in the queue being invalid. By discarding it, we ensure that
        # future iterations aren't also affected.
        records, self._records_to_flush = self._records_to_flush, []

        table = MemberXP.__tablename__
        stmt = f"""
                INSERT INTO {table} (guild_id, user_id, total_xp, last_given_at) VALUES ($1, $2, $3, $4)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET total_xp = {table}.total_xp + EXCLUDED.total_xp,
                last_given_at = EXCLUDED.last_given_at
                WHERE EXCLUDED.last_given_at - {table}.last_given_at >= 60;
                """
        async with db.Pool.acquire() as conn:
            await conn.executemany(stmt, records)

    _flush_records = flush_records.coro  # original function

    @flush_records.before_loop
    async def before_flush_loop(self):
        await self.bot.wait_until_ready()

    def cog_unload(self):
        self.flush_records.cancel()

    @Cog.listener()
    async def on_message(self, message):
        if message.guild is None or message.author.bot:
            return
        self._records_to_flush.append((message.guild.id, message.author.id, 20, message.created_at.timestamp()))

    def _ensure_flush_running(self):
        task = self.flush_records.get_task()
        if task is None or not task.done():  # has not been started or has been started and not stopped
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            logger.warning("Task flushing records was cancelled prematurely, restarting")
        else:
            # exc could be None if the task returns normally, but that would also be an error
            logger.error("Task flushing records failed: %r", exc)
        finally:
            self.flush_records.start()

    @command()
    @guild_only()
    async def rank(self, ctx, member: discord.Member = None):
        """Get a user's ranking on the XP leaderboard.
        If no member is passed, the caller's ranking is shown.
        """
        member = member or ctx.author
        await self._flush_records()

        self._ensure_flush_running()

        # Make Postgres compute the rank for us (need WITH-query so rank() sees records for every user)
        record = await db.Pool.fetchrow(f"""
            WITH ranked_xp AS (
                SELECT user_id, total_xp, rank() OVER (ORDER BY total_xp DESC) FROM {MemberXP.__tablename__}
                WHERE guild_id = $1
            ) SELECT total_xp, rank FROM ranked_xp WHERE user_id = $2;
        """, ctx.guild.id, member.id)

        if record:
            total_xp, rank = record
        else:
            await ctx.send("That user isn't on the leaderboard.")
            return

        embed = discord.Embed(color=member.color)
        embed.description = (f"Level TODO, TODO/TODO XP to level up ({total_xp} total)\n"
                             f"#{rank} in this server")
        embed.set_author(name=member.display_name, icon_url=member.avatar_url_as(format='png', size=64))
        await ctx.send(embed=embed)

    rank.example_usage = """
    `{prefix}rank`: show your ranking
    `{prefix}rank coolgal#1234`: show another user's ranking
    """


class MemberXP(db.DatabaseTable):
    """Database table mapping a guild and user to their XP and related values."""
    __tablename__ = 'levels_member_xp'
    __uniques__ = 'guild_id, user_id'

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

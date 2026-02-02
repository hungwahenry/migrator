#!/usr/bin/env python3
"""
Telegram Group Migration Tool

Migrates users from one Telegram group to another using multiple accounts
with rate limiting and automatic account rotation.
"""

import asyncio
import json
import logging
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from telethon import TelegramClient
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.types import User, Channel, Chat
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserAlreadyParticipantError,
    UserIdInvalidError,
    UserNotMutualContactError,
    UserChannelsTooMuchError,
    ChatWriteForbiddenError,
    ChannelPrivateError,
    UserKickedError,
    UserBannedInChannelError,
    InputUserDeactivatedError,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class AccountConfig:
    """Configuration for a single Telegram account."""
    phone: str
    api_id: int
    api_hash: str
    session_name: str
    client: Optional[TelegramClient] = field(default=None, repr=False)
    is_active: bool = True
    users_added_this_session: int = 0
    cooldown_until: Optional[datetime] = None


@dataclass
class MigrationProgress:
    """Tracks migration progress for resume capability."""
    processed_user_ids: set = field(default_factory=set)
    successful_adds: int = 0
    failed_adds: int = 0
    skipped_privacy: int = 0
    skipped_already_member: int = 0
    last_updated: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'processed_user_ids': list(self.processed_user_ids),
            'successful_adds': self.successful_adds,
            'failed_adds': self.failed_adds,
            'skipped_privacy': self.skipped_privacy,
            'skipped_already_member': self.skipped_already_member,
            'last_updated': self.last_updated
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationProgress':
        return cls(
            processed_user_ids=set(data.get('processed_user_ids', [])),
            successful_adds=data.get('successful_adds', 0),
            failed_adds=data.get('failed_adds', 0),
            skipped_privacy=data.get('skipped_privacy', 0),
            skipped_already_member=data.get('skipped_already_member', 0),
            last_updated=data.get('last_updated')
        )


class AccountPool:
    """Manages multiple Telegram accounts with rotation."""

    def __init__(self, accounts: list[AccountConfig], rate_config: dict):
        self.accounts = accounts
        self.rate_config = rate_config
        self.current_index = 0

    def get_current_account(self) -> Optional[AccountConfig]:
        """Get the current active account, rotating if needed."""
        attempts = 0
        while attempts < len(self.accounts):
            account = self.accounts[self.current_index]

            # Check if account is on cooldown
            if account.cooldown_until:
                if datetime.now() < account.cooldown_until:
                    logger.info(f"Account {account.phone} on cooldown until {account.cooldown_until}")
                    self._rotate()
                    attempts += 1
                    continue
                else:
                    account.cooldown_until = None
                    account.users_added_this_session = 0

            if account.is_active and account.client:
                return account

            self._rotate()
            attempts += 1

        return None

    def _rotate(self):
        """Rotate to the next account."""
        self.current_index = (self.current_index + 1) % len(self.accounts)

    def mark_account_flooded(self, account: AccountConfig, wait_seconds: int):
        """Mark an account as rate-limited."""
        from datetime import timedelta
        account.cooldown_until = datetime.now() + timedelta(seconds=wait_seconds)
        logger.warning(f"Account {account.phone} flooded, cooling down for {wait_seconds}s")
        self._rotate()

    def mark_account_disabled(self, account: AccountConfig):
        """Disable an account (e.g., on PeerFloodError)."""
        account.is_active = False
        logger.error(f"Account {account.phone} disabled due to flood/spam flag")
        self._rotate()

    def increment_add_count(self, account: AccountConfig):
        """Increment the add count and rotate if threshold reached."""
        account.users_added_this_session += 1
        max_users = self.rate_config.get('users_per_account_before_rotate', 25)

        if account.users_added_this_session >= max_users:
            cooldown = self.rate_config.get('cooldown_after_rotate_seconds', 900)
            logger.info(f"Account {account.phone} reached {max_users} adds, rotating with {cooldown}s cooldown")
            self.mark_account_flooded(account, cooldown)

    def get_active_count(self) -> int:
        """Get the number of active accounts."""
        return sum(1 for a in self.accounts if a.is_active)


class TelegramMigrator:
    """Main migration orchestrator."""

    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.rate_config = self.config.get('rate_limit', {})
        self.progress_file = Path(self.config.get('progress_file', 'migration_progress.json'))
        self.progress = self._load_progress()
        self.accounts: list[AccountConfig] = []
        self.account_pool: Optional[AccountPool] = None

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file."""
        with open(config_path, 'r') as f:
            return json.load(f)

    def _load_progress(self) -> MigrationProgress:
        """Load progress from file or create new."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                logger.info(f"Loaded progress: {data['successful_adds']} successful, {len(data['processed_user_ids'])} processed")
                return MigrationProgress.from_dict(data)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Could not load progress file: {e}")
        return MigrationProgress()

    def _save_progress(self):
        """Save current progress to file."""
        self.progress.last_updated = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress.to_dict(), f, indent=2)

    async def initialize_accounts(self):
        """Initialize all Telegram client accounts."""
        for acc_config in self.config['accounts']:
            account = AccountConfig(
                phone=acc_config['phone'],
                api_id=acc_config['api_id'],
                api_hash=acc_config['api_hash'],
                session_name=acc_config['session_name']
            )

            client = TelegramClient(
                account.session_name,
                account.api_id,
                account.api_hash
            )

            # Set flood sleep threshold
            flood_threshold = self.rate_config.get('flood_wait_threshold', 300)
            client.flood_sleep_threshold = flood_threshold

            try:
                await client.start(phone=account.phone)
                account.client = client
                me = await client.get_me()
                logger.info(f"Logged in as {me.first_name} ({account.phone})")
            except Exception as e:
                logger.error(f"Failed to initialize account {account.phone}: {e}")
                account.is_active = False

            self.accounts.append(account)

        self.account_pool = AccountPool(self.accounts, self.rate_config)
        logger.info(f"Initialized {self.account_pool.get_active_count()}/{len(self.accounts)} accounts")

    async def fetch_source_members(self) -> list[User]:
        """Fetch all members from the source group."""
        account = self.account_pool.get_current_account()
        if not account:
            raise RuntimeError("No active accounts available")

        source_group = self.config['source_group']
        logger.info(f"Fetching members from {source_group}...")

        members = []
        try:
            async for user in account.client.iter_participants(source_group):
                # Skip bots and deleted accounts
                if user.bot or user.deleted:
                    continue
                # Skip already processed users
                if user.id in self.progress.processed_user_ids:
                    continue
                members.append(user)
        except Exception as e:
            logger.error(f"Error fetching members: {e}")
            raise

        logger.info(f"Found {len(members)} members to migrate (excluding already processed)")
        return members

    async def add_user_to_destination(self, user: User) -> bool:
        """
        Add a single user to the destination group.
        Returns True if successful, False otherwise.
        """
        account = self.account_pool.get_current_account()
        if not account:
            logger.error("No active accounts available")
            return False

        destination = self.config['destination_group']

        try:
            await account.client(InviteToChannelRequest(
                channel=destination,
                users=[user]
            ))

            self.progress.successful_adds += 1
            self.account_pool.increment_add_count(account)
            logger.info(f"Successfully added {user.first_name} (ID: {user.id}) using {account.phone}")
            return True

        except UserAlreadyParticipantError:
            self.progress.skipped_already_member += 1
            logger.info(f"User {user.first_name} (ID: {user.id}) already in destination group")
            return True  # Consider this a success

        except UserPrivacyRestrictedError:
            self.progress.skipped_privacy += 1
            logger.info(f"User {user.first_name} (ID: {user.id}) has privacy restrictions")
            return False

        except (UserNotMutualContactError, UserChannelsTooMuchError,
                UserKickedError, UserBannedInChannelError, InputUserDeactivatedError) as e:
            self.progress.failed_adds += 1
            logger.warning(f"Cannot add {user.first_name} (ID: {user.id}): {type(e).__name__}")
            return False

        except UserIdInvalidError:
            self.progress.failed_adds += 1
            logger.warning(f"Invalid user ID for {user.first_name} (ID: {user.id})")
            return False

        except FloodWaitError as e:
            logger.warning(f"FloodWaitError: need to wait {e.seconds} seconds")
            self.account_pool.mark_account_flooded(account, e.seconds)
            # Retry with next account
            return await self.add_user_to_destination(user)

        except PeerFloodError:
            logger.error(f"PeerFloodError on account {account.phone} - account may be flagged")
            self.account_pool.mark_account_disabled(account)
            # Retry with next account if available
            if self.account_pool.get_active_count() > 0:
                return await self.add_user_to_destination(user)
            return False

        except (ChatWriteForbiddenError, ChannelPrivateError) as e:
            logger.error(f"Cannot access destination group: {type(e).__name__}")
            raise  # This is a fatal error

        except Exception as e:
            self.progress.failed_adds += 1
            logger.error(f"Unexpected error adding {user.first_name} (ID: {user.id}): {e}")
            return False

    async def run_migration(self):
        """Main migration loop."""
        try:
            # Initialize accounts
            await self.initialize_accounts()

            if self.account_pool.get_active_count() == 0:
                logger.error("No active accounts available. Exiting.")
                return

            # Fetch source members
            members = await self.fetch_source_members()

            if not members:
                logger.info("No members to migrate. Exiting.")
                return

            logger.info(f"Starting migration of {len(members)} users...")
            logger.info(f"Rate limit: {self.rate_config.get('delay_between_adds_min', 60)}-"
                       f"{self.rate_config.get('delay_between_adds_max', 120)}s between adds")

            for i, user in enumerate(members, 1):
                # Check if we have active accounts
                if self.account_pool.get_active_count() == 0:
                    logger.error("All accounts exhausted. Stopping migration.")
                    break

                logger.info(f"Processing user {i}/{len(members)}: {user.first_name} (ID: {user.id})")

                # Add user
                await self.add_user_to_destination(user)

                # Mark as processed regardless of outcome
                self.progress.processed_user_ids.add(user.id)
                self._save_progress()

                # Rate limiting delay
                if i < len(members):  # Don't delay after last user
                    delay_min = self.rate_config.get('delay_between_adds_min', 60)
                    delay_max = self.rate_config.get('delay_between_adds_max', 120)
                    delay = random.uniform(delay_min, delay_max)
                    logger.info(f"Waiting {delay:.1f}s before next add...")
                    await asyncio.sleep(delay)

            # Final summary
            logger.info("=" * 50)
            logger.info("Migration Summary:")
            logger.info(f"  Successful adds: {self.progress.successful_adds}")
            logger.info(f"  Already members: {self.progress.skipped_already_member}")
            logger.info(f"  Privacy restricted: {self.progress.skipped_privacy}")
            logger.info(f"  Failed: {self.progress.failed_adds}")
            logger.info(f"  Total processed: {len(self.progress.processed_user_ids)}")
            logger.info("=" * 50)

        finally:
            # Disconnect all clients
            for account in self.accounts:
                if account.client:
                    await account.client.disconnect()
            self._save_progress()


async def main():
    """Entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Telegram Group Migration Tool')
    parser.add_argument('--config', '-c', default='config.json',
                       help='Path to configuration file (default: config.json)')
    parser.add_argument('--reset-progress', action='store_true',
                       help='Reset migration progress and start fresh')
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        logger.info("Create a config.json file based on config.example.json")
        sys.exit(1)

    if args.reset_progress:
        progress_file = Path('migration_progress.json')
        if progress_file.exists():
            progress_file.unlink()
            logger.info("Progress reset.")

    migrator = TelegramMigrator(str(config_path))
    await migrator.run_migration()


if __name__ == '__main__':
    asyncio.run(main())

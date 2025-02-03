This folder contains the legacy implementation of sharding blob access
configuration. Care has been taken to leave this code contained and reasonably
unmodified

The purpose of keeping the legacy sharding implementation around is to simplify
switching from the old sharding implementation to the new implementation,
components can be switched to the new sharding implementation with a fallback to
the old implementation.

Consumers are expected to switch in a timely manner, at some point this entire
folder will be deleted.
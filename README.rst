**************
 continuwuity
**************

A community-driven `Matrix <https://matrix.org/>`__ homeserver in Rust
######################################################################

continuwuity is a Matrix homeserver written in Rust.
This is my personal experimental fork.

.. ANCHOR: body

|unittest_dev|

|comp_gh| ``(main)``

|comp_gh_dev| ``(dev)``

|comp_fg|

My Project
~~~~~~~~~~

Up til now it's backwards compatible, but who dropped Sync Tokens first, tho.

Optimizations in PDU event storage and read receipt logic will leave it largely
non-backwards compatible, for now.

The main branch is generally the most stable. Some minor regressions with the
`unsigned` field, media linger there (fixed on `dev` branches, but those are
more experimental and not advised to run on your own).

Feature branches are best avoided unless checking with me first.

Complement tests have been added, as well functionality for these:

.. code-block:: text

   ✓  tests/msc3890 (9.129s) [Remotely silence local notifications]
   ✓  tests/msc3967 (9.445s) [Do not require UIA when uploading cross-signing keys]
   ✓  tests/msc4155 (19.007s) [Invite filtering]
   ✓  tests/msc4222 (11.685s) [Adding `state_after` to `/sync`]

   ✓ MSC3266 [Room summaries]
   ✓ MSC3890 [Remotely silence local notifications]
   ✓ MSC4289 [Explicitly privilege room creators]

   TODO:

   - MSC4108 [QR Code login]
   - other complement failures relevant to continuwuity


Ignore the complement results branch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This branch is large, slow to pull, and pollutes ``git lgb --all``.

Add this in ``.git/config`` to suppress fetches:

.. code-block:: ini

  [remote "origin"]
    url = git@github.com:gamesguru/continuwuity.git
    fetch = +refs/heads/*:refs/remotes/origin/*
    fetch = ^refs/heads/_metadata/badges

Run this to purge it from local logs/history:

.. code-block:: shell

  git update-ref -d refs/remotes/origin/_metadata/badges

.. Substitutions for Badges

.. |unittest_dev| image:: https://raw.githubusercontent.com/gamesguru/continuwuity/_metadata/badges/tests-guru-dev-2026-03-27-b1-presence-b2-federation.svg
   :target: https://github.com/gamesguru/continuwuity/actions/workflows/test.yml?query=branch%3Aguru%2Fdev-2026-03-27%2Bb1-presence%2Bb2-federation
   :alt: Unit Tests

.. |comp_gh| image:: https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fgamesguru%2Fcontinuwuity%2F_metadata%2Fbadges%2Fbadge-main.json
   :target: https://github.com/gamesguru/continuwuity/actions/workflows/complement.yml?query=branch%3Amain
   :alt: Complement Tests (GitHub)

.. |comp_gh_dev| image:: https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fgamesguru%2Fcontinuwuity%2F_metadata%2Fbadges%2Fbadge-guru-dev-2026-03-27-b1-presence-b2-federation.json
   :target: https://github.com/gamesguru/continuwuity/actions/workflows/complement.yml?query=branch%3Aguru%2Fdev-2026-03-27%2Bb1-presence%2Bb2-federation
   :alt: Complement Tests (dev)

.. |comp_fg| image:: https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fgamesguru%2Fcontinuwuity%2F_metadata%2Fbadges%2Fbadge-main-upstream.json
   :target: https://forgejo.ellis.link/gamesguru/continuwuity/actions?workflow=complement.yml&actor=0&status=0
   :alt: Complement Tests (Forge)

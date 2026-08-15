# continuwuity

## A community-driven [Matrix](https://matrix.org/) homeserver in Rust

[continuwuity] is a Matrix homeserver written in Rust.
It's the official community continuation of the [conduwuit](https://github.com/girlbossceo/conduwuit) homeserver.

<!-- ANCHOR: body -->

[![Stars](https://forgejo.ellis.link/continuwuation/continuwuity/badges/stars.svg?style=flat)](https://forgejo.ellis.link/continuwuation/continuwuity/stars) [![Issues](https://forgejo.ellis.link/continuwuation/continuwuity/badges/issues/open.svg?style=flat)](https://forgejo.ellis.link/continuwuation/continuwuity/issues?state=open) [![Pull Requests](https://forgejo.ellis.link/continuwuation/continuwuity/badges/pulls/open.svg?style=flat)](https://forgejo.ellis.link/continuwuation/continuwuity/pulls?state=open)

[![GitHub](https://img.shields.io/badge/GitHub-mirror-blue?style=flat&logo=github&labelColor=fff&logoColor=24292f)](https://github.com/continuwuity/continuwuity) [![Stars](https://img.shields.io/github/stars/continuwuity/continuwuity?style=flat)](https://github.com/continuwuity/continuwuity/stargazers)
[![GitLab](https://img.shields.io/badge/GitLab-mirror-blue?style=flat&logo=gitlab&labelColor=fff)](https://gitlab.com/continuwuity/continuwuity) [![Stars](https://img.shields.io/gitlab/stars/continuwuity/continuwuity?style=flat)](https://gitlab.com/continuwuity/continuwuity/-/starrers)
[![Codeberg](https://img.shields.io/badge/Codeberg-mirror-2185D0?style=flat&logo=codeberg&labelColor=fff)](https://codeberg.org/continuwuity/continuwuity) [![Stars](https://codeberg.org/continuwuity/continuwuity/badges/stars.svg?style=flat)](https://codeberg.org/continuwuity/continuwuity/stars)

[![Complement Tests (GitHub)](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fgamesguru%2Fcontinuwuity%2F_metadata%2Fbadges%2Fbadge-main.json)](https://github.com/gamesguru/continuwuity/actions/workflows/complement.yml?query=branch%3Amain)

[![Complement Tests (Forge)](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fgamesguru%2Fcontinuwuity%2F_metadata%2Fbadges%2Fbadge-main-upstream.json)](https://github.com/gamesguru/continuwuity/actions/workflows/complement.yml?query=branch%3Amain-upstream)

### Why does this exist?

The original conduwuit project has been archived and is no longer maintained. Rather than letting this Rust-based Matrix homeserver disappear, a group of community contributors have forked the project to continue its development, fix outstanding issues, and add new features.

We aim to provide a stable, well-maintained alternative for current conduwuit users and welcome newcomers seeking a lightweight, efficient Matrix homeserver.

### Who are we?

We are a group of Matrix enthusiasts, developers and system administrators who have used conduwuit and believe in its potential. Our team includes both previous
contributors to the original project and new developers who want to help maintain and improve this important piece of Matrix infrastructure.

We operate as an open community project, welcoming contributions from anyone interested in improving continuwuity.

### What is Matrix?

[Matrix](https://matrix.org) is an open, federated, and extensible network for
decentralized communication. Users from any Matrix homeserver can chat with users from all
other homeservers over federation. Matrix is designed to be extensible and built on top of.
You can even use bridges such as Matrix Appservices to communicate with users outside of Matrix, like a community on Discord.

### What are the project's goals?

Continuwuity aims to:

- Maintain a stable, reliable Matrix homeserver implementation in Rust
- Improve compatibility and specification compliance with the Matrix protocol
- Fix bugs and performance issues from the original conduwuit
- Add missing features needed by homeserver administrators
- Provide comprehensive documentation and easy deployment options
- Create a sustainable development model for long-term maintenance
- Keep a lightweight, efficient codebase that can run on modest hardware

### Can I try it out?

Check out the [documentation](https://continuwuity.org) for installation instructions.

### What are we working on?

We're working our way through all of the issues in the [Forgejo project](https://forgejo.ellis.link/continuwuation/continuwuity/issues).

- [Packaging & availability in more places](https://forgejo.ellis.link/continuwuation/continuwuity/issues/747)
- [Appservices bugs & features](https://forgejo.ellis.link/continuwuation/continuwuity/issues?q=&type=all&state=open&labels=178&milestone=0&assignee=0&poster=0)
- [Improving compatibility and spec compliance](https://forgejo.ellis.link/continuwuation/continuwuity/issues?labels=119)
- Automated testing
- [Admin API](https://forgejo.ellis.link/continuwuation/continuwuity/issues/748)
- [Policy-list controlled moderation](https://forgejo.ellis.link/continuwuation/continuwuity/issues/750)

### Can I migrate my data from x?

- Conduwuit: No longer strictly compatible. This fork pre-emptively truncates the synctoken table, and deeper database changes (planned for the `guru/dev-2026-...` branch shortly) are underway.
- Conduit: No, database is now incompatible
- Grapevine: No, database is now incompatible
- Dendrite: No
- Synapse: No

We haven't written up a guide on migrating from incompatible homeservers yet. Reach out to us if you need to do this!

# Contributing Guidelines

There are many ways to contribute to the 3DBAG pipeline, from bug fixes to performance optimizations, new feature design, and new tests. This guide offers a walkthrough of the general process for proposing and implementing a new feature, as well as some tips for engaging with our community.

**IMPORTANT**: If you have simply found a bug but you do not want to solve it yourself, please report it in detail by [creating an issue](https://github.com/3DBAG/3dbag-pipeline/issues).

## Contribution Stages

### Idea Discussion

If you have an idea for a feature or for fixing a bug, start by discussing it with the community on [Zulip](https://3dbag.zulipchat.com).
This will help flesh out the initial idea, allowing the community to provide early feedback before you move on to writing code.

Once the concept is solid, you can start coding.


### Implementation

You can start implementing your idea or fix by making a feature branch from the `develop` branch.

The `develop` branch is where the active development occurs and external contributors should base their work here.
[See here for more information about our workflow and our branches](/docs/development/code.md#branches)

It is expected that you will provide sufficient documentation and tests for your code.
Additionally, your code should be formatted according to the [PEP 8 style guide](https://peps.python.org/pep-0008/).
Please test your implementation thoroughly before opening a PR against the `develop` branch.

Each PR is automatically checked for:

- Formatting conformance
- Unit tests
- Integration tests

Only PRs that pass all checks will be considered for reviewing and will be eventually merged.

### Refinement

Based on internal testing and testing against other new features, your contribution may need adjustments or refinements.
You might be asked to improve aspects such as code quality, performance, or compatibility with existing functionality.

Once a feature has proven stable on the `develop` branch, it may be promoted to a production candidate.


### Release:

If the production candidate passes testing in the `production` branch, it will be merged into `master` and tagged for release.

Congratulations, your contribution is now part of the stable 3DBAG pipeline!


## Additional Notes for New Contributors

**Start Small:** New contributors are encouraged to start with small issues or bug fixes before proposing larger features. 
**Ask for Help:** If you need help or aren’t sure about any steps, don’t hesitate to post a question on [Zulip](https://3dbag.zulipchat.com). The community is here to support you!
**Stay Updated:** Follow repository updates to stay informed of any changes to the branching strategy or contribution processes. We are still figuring it out - so this might happen quite often.
**Documentation:** Ensure all new features or changes are well-documented. Good documentation helps future contributors and maintainers understand the intent and functionality of your code. You can find the instructions on [how to generate the documentation here](documentation.md).

Happy contributing! We appreciate your involvement in the 3DBAG pipeline project.
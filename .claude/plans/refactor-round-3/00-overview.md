# Big refactoring round 3

The main goal of this refactoring round is that the 3dbag-pipeline is ready for future improvements.

First and foremost, this means that we can have a uniform feature-based pipeline throughout, and that the party walls calculation is refactor to take CityJSONFeatures as input, instead of CityJSON tiles.

Second, we have fully integrated the floor estimation process, including the building type calculation, so that we can recreate and update all parts of the pipeline from the baseregistrations, instead of having to import additional sources.

These changes open up the way towards changes like a fixed partition scheme, high-performance storage formats and similar.

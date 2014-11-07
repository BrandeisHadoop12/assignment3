## Initial Implementation ##

We removed stop words ahead of time, in order to reduce computing load and time. We reused the stemmer and lemmatiser from assignment assignment, save for tweaks that accommodated for changes in the input file itself.

## Hard Clustering ##

As a refresher, k-means minimizes the overall sum of squared distance of every instance from the cluster center. Our implementation is `KMeansReccomender.java`.

A sample of the results follows:

## Soft Clustering (Fuzzy K-means) ##

Our fuzzy k-means implementation is located in `FuzzyKMeansRecommender.java`.
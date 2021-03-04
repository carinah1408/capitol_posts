library(tidytext)
library(tidyverse)

posts <- read_csv("./data/Parler_posts.csv")

# reduce dataset to only those entries that were posted "4 days ago" which was dated by the researcher to the 6th of January
posts <- posts %>%
  dplyr::filter(post_timestamp == "4 days ago") %>%
  dplyr::select(-post_image)

# subset of 1% of the data set for pre-analysis
one_perc <- head(posts, n = 680)

# remove NAs cases
one_perc <- one_perc %>%
  dplyr::filter(!is.na(post_text))

# remove doubled entries
one_perc <- one_perc %>%
  dplyr::filter(!duplicated(post_text))

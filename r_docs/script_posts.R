library(tidytext)
library(tidyverse)
library(ndjson)

posts <- read_csv("./data/Parler_posts.csv")
big_posts00 <- stream_in("./data/parler_data000000000000.ndjson")
big_posts01 <- stream_in("./data/parler_data000000000001.ndjson")
big_posts02 <- stream_in("./data/parler_data000000000002.ndjson")
big_posts08 <- stream_in("./data/parler_data000000000008.ndjson")

## posts data set

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

# transform into csv
write.csv(one_perc, "post_1%.csv")


## big posts data set

big_posts_00 <- big_posts_00 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06"))

big_posts <- big_posts %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06"))

big_posts01 <- big_posts01 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06"))

big_posts02 <- big_posts02 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06"))

# transform into csv
write.csv(big_posts, "parler_data000000000008_20210106.csv")

write.csv(big_posts01, "parler_data000000000001_20210106.csv")

write.csv(big_posts02, "parler_data000000000002_20210106.csv")



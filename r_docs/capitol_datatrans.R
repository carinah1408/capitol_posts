library(tidyverse)
library(ndjson)

# read in data sets
posts <- read_csv("./data/Parler_posts.csv")

big_posts00 <- stream_in("./data/parler_data000000000000.ndjson")
big_posts01 <- stream_in("./data/parler_data000000000001.ndjson")
big_posts02 <- stream_in("./data/parler_data000000000002.ndjson")
big_posts03 <- stream_in("./data/parler_data000000000003.ndjson")
big_posts04 <- stream_in("./data/parler_data000000000004.ndjson")
big_posts05 <- stream_in("./data/parler_data000000000005.ndjson")
big_posts06 <- stream_in("./data/parler_data000000000006.ndjson")
big_posts07 <- stream_in("./data/parler_data000000000007.ndjson")

# read in meta data (big posts)
big_meta00 <- stream_in("./data/parler_user000000000000.ndjson")

big_meta01 <- stream_in("./data/parler_user000000000001.ndjson")
big_meta02 <- stream_in("./data/parler_user000000000002.ndjson")
big_meta03 <- stream_in("./data/parler_user000000000003.ndjson")
big_meta04 <- stream_in("./data/parler_user000000000004.ndjson")
big_meta05 <- stream_in("./data/parler_user000000000005.ndjson")
big_meta06 <- stream_in("./data/parler_user000000000006.ndjson")
big_meta07 <- stream_in("./data/parler_user000000000007.ndjson")
big_meta08 <- stream_in("./data/parler_user000000000008.ndjson")

## big posts data set

# select relevant columns
big_meta00 <- big_meta00 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta01 <- big_meta01 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta02 <- big_meta02 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta03 <- big_meta03 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta04 <- big_meta04 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta05 <- big_meta05 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta06 <- big_meta06 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta07 <- big_meta07 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta08 <- big_meta08 %>%
  dplyr::select(id, bio, human, username, user_followers)

# reduce dataset to only those entries that were posted on 6th of January
big_posts00 <- big_posts00 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06"))

big_posts01 <- big_posts01 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts02 <- big_posts02 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts03 <- big_posts03 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts04 <- big_posts04 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts05 <- big_posts05 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts06 <- big_posts06 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts07 <- big_posts07 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

# merge dataset based on common username and remove "human == FALSE"

data08 <- inner_join(big_posts08, big_meta00) %>%
  dplyr::filter(human == TRUE) 

# transform into csv
write.csv(data07, "data07.csv")




## posts data set

# reduce dataset to only those entries that were posted "4 days ago" which was dated by the researcher to the 6th of January
posts <- posts %>%
  dplyr::filter(post_timestamp == "4 days ago") %>%
  dplyr::select(-post_image)

# remove NAs cases, duplicates, and only select those posts with "#"
posts <- posts %>%
  dplyr::mutate(post_text = as.character(post_text)) %>%
  dplyr::filter(!is.na(post_text)) %>%
  dplyr::filter(!duplicated(post_text)) %>%
  dplyr::filter(grepl("#", post_text)) # resulted in 2,726 obs.

# subset of 5% of the data set for pre-analysis
five_perc <- head(posts, n = 136)

# transform into csv
write.csv(five_perc, "post_5%.csv")



## investigate both data sets for similarities in usernames

# remove @ in five_perc author_username
five_perc$author_username = as.character(gsub("\\@", "", five_perc$author_username))

# compare datasets
intersect(five_perc$author_name, data01$username)



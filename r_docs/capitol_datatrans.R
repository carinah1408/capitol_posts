library(tidyverse)
library(ndjson)
library(plyr)

## datasets
posts <- read_csv("./data/Parler_posts.csv") # dataset taken from https://github.com/sbooeshaghi/parlertrick/blob/main/data/all_posts.csv.gz
posts2 <- read_csv("./data/out.csv") # dataset scraped from html files (data_no_video.tar.gz)
posts2 <- read_csv("./data/posts2.csv") 

# datasets from https://zenodo.org/record/4442460#.YEzn7J37SMp
big_posts00 <- stream_in("./data/parler_data000000000000.ndjson")
big_posts01 <- stream_in("./data/parler_data000000000001.ndjson")
big_posts02 <- stream_in("./data/parler_data000000000002.ndjson")

big_posts03 <- stream_in("./data/parler_data000000000003.ndjson")
big_posts04 <- stream_in("./data/parler_data000000000004.ndjson")
big_posts05 <- stream_in("./data/parler_data000000000005.ndjson")
big_posts06 <- stream_in("./data/parler_data000000000006.ndjson")
big_posts07 <- stream_in("./data/parler_data000000000007.ndjson")

# meta data (big posts)
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
  dplyr::select(bio, human, username, user_followers)

big_meta01 <- big_meta01 %>%
  dplyr::select(bio, human, username, user_followers)

big_meta02 <- big_meta02 %>%
  dplyr::select(bio, human, username, user_followers)

big_meta03 <- big_meta03 %>%
  dplyr::select(bio, human, username, user_followers)

big_meta04 <- big_meta04 %>%
  dplyr::select(bio, human, username, user_followers)

big_meta05 <- big_meta05 %>%
  dplyr::select(bio, human, username, user_followers)

big_meta06 <- big_meta06 %>%
  dplyr::select(bio, human, username, user_followers)

big_meta07 <- big_meta07 %>%
  dplyr::select(id, bio, human, username, user_followers)

big_meta08 <- big_meta08 %>%
  dplyr::select(id, bio, human, username, user_followers)

# reduce datasets to only those entries that were posted on 6th of January
big_posts00 <- big_posts00 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06"))

big_posts01 <- big_posts01 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts02 <- big_posts02 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts03 <- big_posts03 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts04 <- big_posts04 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts05 <- big_posts05 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts06 <- big_posts06 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

big_posts07 <- big_posts07 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

# merge datasets based on common username and remove "human == FALSE"

data03 <- inner_join(big_posts03, big_meta00) 

# transform into csv
write.csv(data03, "data03.csv")




## posts data set

# reduce dataset to only those entries that were posted "4 days ago" which was dated by the researcher to the 6th of January
posts <- posts %>%
  dplyr::filter(post_timestamp >= "4 days ago" & post_timestamp <= "5 days ago") 

# remove NAs cases
posts2 <- posts2 %>%
  dplyr::mutate(post_text = as.character(post_text)) %>%
  dplyr::filter(!is.na(post_text))

# create merged column (author_username + post_text)
posts2$author_username_posts_text <- paste(posts2$author_username, posts2$post_text)

# remove duplicates in new column
posts2 <- ddply(posts2,.(author_username_posts_text), head, n = 1)

# select only those posts that contain "#"
posts2 <- posts2 %>%
  dplyr::filter(grepl("#", post_text)) # results in 8,564 obs

# export csv
write.csv(posts2, "posts2.csv")

# randomly select 5% of the data set for pre-analysis

five_perc <- posts2[sample(1:nrow(posts2), 428), ]

# transform into csv
write.csv(five_perc, "post_5%_new.csv")


## investigate data sets for similarities in usernames (bots)

# read in data from edited datasets
data01 <- read_csv("data01.csv")

# remove @ in five_perc author_username
posts2$author_username = as.character(gsub("\\@", "", posts2$author_username))

# compare datasets
intersect(posts2$author_username, data03$username)

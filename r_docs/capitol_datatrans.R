library(tidyverse)
library(ndjson)
library(plyr)

## datasets

# recent Parler scrape
posts0.1 <- read_csv("./data/Parler_posts.csv") # dataset taken from https://github.com/sbooeshaghi/parlertrick/blob/main/data/all_posts.csv.gz
posts0.2 <- read_csv("./data/out.csv") # newly scraped dataset from html files (data_no_video.tar.gz)

# datasets from https://zenodo.org/record/4442460#.YEzn7J37SMp
big_posts00 <- stream_in("./data/parler_data000000000000.ndjson") # no similarities were established with this dataset
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

# already prepared datasets (see below) for further merging
posts <- read_csv("./data/posts.csv") 

big_meta00 <- read_csv("./data/big_meta00.csv")
big_meta01 <- read_csv("./data/big_meta01.csv")

big_posts01 <- read_csv("./data/big_posts01.csv")
big_posts02 <- read_csv("./data/big_posts02.csv")
big_posts03 <- read_csv("./data/big_posts03.csv")
big_posts04 <- read_csv("./data/big_posts04.csv")
big_posts05 <- read_csv("./data/big_posts05.csv")
big_posts06 <- read_csv("./data/big_posts06.csv")
big_posts07 <- read_csv("./data/big_posts07.csv")

data01 <- read_csv("./data/data01.csv")
data02 <- read_csv("./data/data02.csv")
data03 <- read_csv("./data/data03.csv")
data04 <- read_csv("./data/data04.csv")
data05 <- read_csv("./data/data05.csv")
data06 <- read_csv("./data/data06.csv")
data07 <- read_csv("./data/data07.csv")
data11 <- read_csv("./data/data11.csv")
data12 <- read_csv("./data/data12.csv")
data13 <- read_csv("./data/data13.csv")
data14 <- read_csv("./data/data14.csv")
data15 <- read_csv("./data/data15.csv")
data16 <- read_csv("./data/data16.csv")
data17 <- read_csv("./data/data17.csv")


## "big posts" data set

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

# write csv file of meta dataset
write.csv(big_meta01, "big_meta01.csv")


# reduce datasets (only  entries from 6th of January)
big_posts00 <- big_posts00 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06"))

big_posts01 <- big_posts01 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

write.csv(big_posts01, "big_posts01.csv")

big_posts02 <- big_posts02 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

write.csv(big_posts02, "big_posts02.csv")

big_posts03 <- big_posts03 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

write.csv(big_posts03, "big_posts03.csv")

big_posts04 <- big_posts04 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

write.csv(big_posts04, "big_posts04.csv")

big_posts05 <- big_posts05 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

write.csv(big_posts05, "big_posts05.csv")

big_posts06 <- big_posts06 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

write.csv(big_posts06, "big_posts06.csv")

big_posts07 <- big_posts07 %>%
  dplyr::filter(str_detect(createdAtformatted, "2021-01-06")) %>%
  dplyr::select(id, body, createdAtformatted, datatype, verified, hashtags.0, impressions, reposts, upvotes, score, username, hashtags.1, hashtags.2)

write.csv(big_posts07, "big_posts07.csv")

# merge datasets based on common username

data14 <- inner_join(big_posts04, big_meta01) 

# transform into csv
write.csv(data14, "data14.csv")


## "posts" data set

# reduce dataset (only entries posted "4 days ago" and "5 days ago" = dated by the researcher to the 6th of January)
posts <- posts %>%
  dplyr::filter(post_timestamp >= "4 days ago" & post_timestamp <= "5 days ago") 

# remove NAs 
posts <- posts %>%
  dplyr::mutate(post_text = as.character(post_text)) %>%
  dplyr::filter(!is.na(post_text))

# create merged column (author_username + post_text)
posts$author_username_posts_text <- paste(posts$author_username, posts$post_text)

# remove duplicates in new column
posts <- ddply(posts,.(author_username_posts_text), head, n = 1)

# select posts that contain "#"
posts <- posts %>%
  dplyr::filter(grepl("#", post_text)) # results in 8,564 obs

# export csv
write.csv(posts, "posts.csv")

# randomly select 5% of the data set for pre-analysis

five_perc <- posts2[sample(1:nrow(posts2), 428), ]

# transform into csv
write.csv(five_perc, "posts_5%.csv")


## investigate data sets for similarities in usernames and post ID (bots)

# remove @ in five_perc author_username
posts2$author_username = as.character(gsub("\\@", "", posts2$author_username))

# compare datasets
intersect(posts2$author_username, data03$username)

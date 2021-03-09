library(tidyverse)

data01 <- read_csv("data01.csv")
data02 <- read_csv("data02.csv")
data03 <- read_csv("data03.csv")
data04 <- read_csv("data04.csv")
data05 <- read_csv("data05.csv")
data06 <- read_csv("data06.csv")
data07 <- read_csv("data07.csv")

data <- rbind(data01, data02, data03, data04, data05, data06, data07) 

write.csv(data, "data_complete_meta00.csv")

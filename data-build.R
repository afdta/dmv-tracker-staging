library(tidyverse)
library(jsonlite)
library(readxl)
#library(sf)

params <- list(path_to_project_root = "", rdata_file_name = "gwp-source-data 7.rda", metadata_file_name = "metadata.xlsx")

#params$path_to_project_root = "~/Projects/Brookings/dmv-tracker-staging"


#### load and process data
load(file.path(params$path_to_project_root, "input-data", params$rdata_file_name) )

#transform M1, M2, etc into standard months
m2d <- tibble(qm = c("M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M10", "M11", "M12", "Q1", "Q2", "Q3", "Q4","A1"),
              mm = c("01", "02", "03", "04", "05", "06", "07", "08", "09",  "10",  "11",  "12", "03", "06", "09", "12","12"))

#merge on months and generate proper date string
all <- meta %>% mutate(qm = sub("^[0-9]{4}","",level.date)) %>% inner_join(m2d, by="qm") %>%
  mutate(date=paste(substr(level.date, 1, 4), mm, 15, sep="-")) %>%
  select(geoid=area, key=level.key, date, value) %>%
  mutate(value_=value, value=case_when(
    key=="unempr-sa-total" ~ value/100,
    key=="unempr-sa-white" ~ value/100, 
    key=="unempr-sa-black" ~ value/100, 
    key=="unempr-sa-poc" ~ value/100, 
    key=="credit-constrained-total" ~ value/100, 
    key=="credit-constrained-nearprime" ~ value/100, 
    key=="credit-constrained-subprime" ~ value/100, 
    key=="bachelors-degree" ~ value/100,
    key=="ba-plus" ~ value/100,
    key=="hs-plus" ~ value/100,
    key=="graduate-degree" ~ value/100,
    key=="some-college-aa" ~ value/100,
    key=="less-than-hs" ~ value/100,
    key=="high-school" ~ value/100,
    key=="commercial-occupancy-office" ~ value/100,
    key=="commercial-occupancy-retail" ~ value/100,
    key=="foreign-born" ~ value/100,
    .default=value
    )
  )

diffs <- all %>% filter(value_ != value)
table(diffs$key)

for (k in unique(all$key)){
  write_json(all%>%filter(key==k), file.path(params$path_to_project_root, "output-data", paste0(k,".json")), digits=5, pretty=TRUE, na="null")
}


#### metadata
keysindata = unique(meta$level.key) #what keys do we actually have in the data?

metadata0 <- read_xlsx(file.path(params$path_to_project_root, "input-data", params$metadata_file_name))

topics <- metadata0 %>% select(name=topic.name, topic=topic.key) %>% unique()
subtopics <- metadata0 %>% select(topic=topic.key, subtopic=metric.key, name=metric.name) %>% unique()

metadata1 <- metadata0 %>% select(topic=topic.key, subtopic=metric.key, def=metrics.definition, source=metrics.source,
                                  name=level.name, key=level.key, interval=level.interval,
                                 format=level.format.base, format_change=level.format.change, note=level.format.notes.name,
                                 start_=level.date_range.start, end_=level.date_range.end,
                                 last_updated=vintage) %>% 
                          mutate(start_qm = sub("^[0-9]{4}","",start_), end_qm = sub("^[0-9]{4}","",end_)) %>%
                          inner_join(m2d %>% rename(start_qm=qm, start_mm=mm), by="start_qm") %>%
                          inner_join(m2d %>% rename(end_qm=qm, end_mm=mm), by="end_qm") %>%
                          mutate(start=paste(substr(start_, 1, 4), start_mm, 15, sep="-")) %>%
                          mutate(end=paste(substr(end_, 1, 4), end_mm, 15, sep="-")) %>%
                          mutate(indata = key %in% keysindata)

subsindata <- metadata1 %>% group_by(subtopic) %>% summarise(count = sum(indata)) %>% filter(count > 0)
                          
metadata <- list(
  topics=topics, 
  subtopics=subtopics %>% filter(subtopic %in% subsindata$subtopic), 
  indicators=metadata1 %>% filter(indata) %>% select(topic:note, last_updated, start, end)
  )

write_json(metadata, file.path(params$path_to_project_root, "output-data", "metadata.json"), pretty=TRUE, na="null")       

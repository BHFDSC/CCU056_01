---
title: "ccu056_r_pipeline_01_supplementary"
output: html_document
date: "2023-11-28"
---

Population Denominators

```{r, setup, warning=FALSE, error=FALSE, message=FALSE,eval=TRUE, echo=FALSE}


#libraries
library(odbc)
library(DBI)
library(tidyverse)
library(glue)
library(kableExtra)

#connect
#con <- DBI::dbConnect(odbc:::odbc(), "databricks")

con <- DBI::dbConnect(odbc::odbc(), dsn = "databricks", HTTPPath = "sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy", 
    PWD = ""
      #rstudioapi::askForPassword("Please enter Databricks PAT")
    )

#parameters
dsa = "dsa_391419_j3w9t_collab"
proj = "ccu056"
select_all = glue("SELECT * FROM ",dsa,".")
select_all_proj = glue("SELECT * FROM ",dsa,".",proj,"_")


```

```{r, parameters, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


pop_ethnicity_2021 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_2021_ethnicity_age_sex")))
pop_ethnicity_2011 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_2011_ethnicity_age_sex")))
pop_ethnicity_2001 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_2001_ethnicity_age_sex")))

pop_lsoa_2020 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_mid_2020_age_lsoa_sex")))
pop_lsoa_2017 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_mid_2017_age_lsoa_sex")))
pop_lsoa_2011 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_mid_2011_age_lsoa_sex")))
pop_lsoa_2002 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_mid_2002_age_lsoa_sex")))


pop_lsoa_2017 = pop_lsoa_2017 %>%
  mutate(n=as.numeric((str_replace(n,",",""))))

lsoa_imd_mapping = dbGetQuery(con,sprintf(glue(select_all,"hds_cur_lsoa_2011_imd_lookup")))
lsoa_imd_long = dbGetQuery(con,sprintf(glue(select_all,"hds_cur_lsoa_imd_lookup_long")))


```


# LSOA Population Datasets

Will derive populations for the following groups:

Quintiles and Age
Sex and Age
Quintiles, Sex and Age


```{r}

# LSOA Year 2001 - IMD Years 2004,2007,2010
# LSOA Year 2011 - IMD Years 2015,2019

# Pop 2020 - IMD 2019
# Pop 2017 - IMD 2015
# Pop 2011 - IMD 2010
# Pop 2002 - IMD 2004

pop_lsoa_2020 = pop_lsoa_2020 %>%
  select(lsoa_code,sex,age,n) %>%
  mutate(age_group = case_when(
    `age` <18 ~ "<18",
    `age` >=18 & age <25 ~ "18-24",
    `age` >=25 & age <30 ~ "25-29",
    `age` >=30 & age <35 ~ "30-34",
    `age` >=35 & age <40 ~ "35-39",
    `age` >=40 & age <45 ~ "40-44",
    `age` >=45 & age <50 ~ "45-49",
    `age` >=50 & age <55 ~ "50-54",
    `age` >=55 & age <60 ~ "55-59",
    `age` >=60 & age <65 ~ "60-64",
    `age` >=65 & age <70 ~ "65-69",
    `age` >=70 & age <75 ~ "70-74",
    `age` >=75 & age <80 ~ "75-79",
    `age` >=80 & age <85 ~ "80-84",
    `age` >=85 & age <90 ~ "85-89",
    `age` >=90 ~ ">=90"
  )
  )   %>% 
  # add on Quintiles to LSOAs
  left_join(
    (lsoa_imd_long),
    by=c("lsoa_code"="LSOA")
  )


pop_lsoa_2017 = pop_lsoa_2017 %>%
  select(lsoa_code,sex,age,n) %>%
  mutate(age_group = case_when(
    `age` <18 ~ "<18",
    `age` >=18 & age <25 ~ "18-24",
    `age` >=25 & age <30 ~ "25-29",
    `age` >=30 & age <35 ~ "30-34",
    `age` >=35 & age <40 ~ "35-39",
    `age` >=40 & age <45 ~ "40-44",
    `age` >=45 & age <50 ~ "45-49",
    `age` >=50 & age <55 ~ "50-54",
    `age` >=55 & age <60 ~ "55-59",
    `age` >=60 & age <65 ~ "60-64",
    `age` >=65 & age <70 ~ "65-69",
    `age` >=70 & age <75 ~ "70-74",
    `age` >=75 & age <80 ~ "75-79",
    `age` >=80 & age <85 ~ "80-84",
    `age` >=85 & age <90 ~ "85-89",
    `age` >=90 ~ ">=90"
  )
  )   %>% 
  # add on Quintiles to LSOAs
  left_join(
    (lsoa_imd_long),
    by=c("lsoa_code"="LSOA")
  )


pop_lsoa_2011 = pop_lsoa_2011 %>%
  select(lsoa_code,sex,age,n) %>%
  mutate(age_group = case_when(
    `age` <18 ~ "<18",
    `age` >=18 & age <25 ~ "18-24",
    `age` >=25 & age <30 ~ "25-29",
    `age` >=30 & age <35 ~ "30-34",
    `age` >=35 & age <40 ~ "35-39",
    `age` >=40 & age <45 ~ "40-44",
    `age` >=45 & age <50 ~ "45-49",
    `age` >=50 & age <55 ~ "50-54",
    `age` >=55 & age <60 ~ "55-59",
    `age` >=60 & age <65 ~ "60-64",
    `age` >=65 & age <70 ~ "65-69",
    `age` >=70 & age <75 ~ "70-74",
    `age` >=75 & age <80 ~ "75-79",
    `age` >=80 & age <85 ~ "80-84",
    `age` >=85 & age <90 ~ "85-89",
    `age` >=90 ~ ">=90"
  )
  )   %>% 
  # add on Quintiles to LSOAs
  left_join(
    (lsoa_imd_long),
    by=c("lsoa_code"="LSOA")
  )


pop_lsoa_2002 = pop_lsoa_2002 %>%
  select(lsoa_code,sex,age,n) %>%
  mutate(age_group = case_when(
    `age` <18 ~ "<18",
    `age` >=18 & age <25 ~ "18-24",
    `age` >=25 & age <30 ~ "25-29",
    `age` >=30 & age <35 ~ "30-34",
    `age` >=35 & age <40 ~ "35-39",
    `age` >=40 & age <45 ~ "40-44",
    `age` >=45 & age <50 ~ "45-49",
    `age` >=50 & age <55 ~ "50-54",
    `age` >=55 & age <60 ~ "55-59",
    `age` >=60 & age <65 ~ "60-64",
    `age` >=65 & age <70 ~ "65-69",
    `age` >=70 & age <75 ~ "70-74",
    `age` >=75 & age <80 ~ "75-79",
    `age` >=80 & age <85 ~ "80-84",
    `age` >=85 & age <90 ~ "85-89",
    `age` >=90 ~ ">=90"
  )
  )   %>% 
  # add on Quintiles to LSOAs
  left_join(
    (lsoa_imd_long),
    by=c("lsoa_code"="LSOA")
  )



```


```{r, populationLSOA}

# 2020

pop_lsoa_2020_mapped_quintile_age = pop_lsoa_2020 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2019) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, age_group) %>%
  summarise(pop_lsoa_var_quintile_age=sum(n))


pop_lsoa_2020_mapped_sex_age = pop_lsoa_2020 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2019) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(sex, age_group) %>%
  summarise(pop_lsoa_var_sex_age=sum(n))


pop_lsoa_2020_mapped_quintile_sex_age = pop_lsoa_2020 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2019) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, sex, age_group) %>%
  summarise(pop_lsoa_var_quintile_sex_age=sum(n))


# 2017

pop_lsoa_2017_mapped_quintile_age = pop_lsoa_2017 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2015) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, age_group) %>%
  mutate(n=as.numeric(n)) %>%
  summarise(pop_lsoa_var_quintile_age=sum(n))


pop_lsoa_2017_mapped_sex_age = pop_lsoa_2017 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2015) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(sex, age_group) %>%
  mutate(n=as.numeric(n)) %>%
  summarise(pop_lsoa_var_sex_age=sum(n))


pop_lsoa_2017_mapped_quintile_sex_age = pop_lsoa_2017 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2015) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, sex, age_group) %>%
  mutate(n=as.numeric(n)) %>%
  summarise(pop_lsoa_var_quintile_sex_age=sum(n))


# 2011

pop_lsoa_2011_mapped_quintile_age = pop_lsoa_2011 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2010) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, age_group) %>%
  mutate(n=as.numeric(n)) %>%
  summarise(pop_lsoa_var_quintile_age=sum(n))


pop_lsoa_2011_mapped_sex_age = pop_lsoa_2011 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2010) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(sex, age_group) %>%
  mutate(n=as.numeric(n)) %>%
  summarise(pop_lsoa_var_sex_age=sum(n))


pop_lsoa_2011_mapped_quintile_sex_age = pop_lsoa_2011 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2010) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, sex, age_group) %>%
  mutate(n=as.numeric(n)) %>%
  summarise(pop_lsoa_var_quintile_sex_age=sum(n))


# 2002

pop_lsoa_2002_mapped_quintile_age = pop_lsoa_2002 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2004) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, age_group) %>%
  summarise(pop_lsoa_var_quintile_age=sum(n))


pop_lsoa_2002_mapped_sex_age = pop_lsoa_2002 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2004) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(sex, age_group) %>%
  summarise(pop_lsoa_var_sex_age=sum(n))


pop_lsoa_2002_mapped_quintile_sex_age = pop_lsoa_2002 %>%
  filter(str_starts(lsoa_code,"E")) %>% #removes Wales LSOAs
  filter(IMD_YEAR == 2004) %>% #filtering IMD Year as close as possible to the Population Year
  group_by(QUINTILE, sex, age_group) %>%
  summarise(pop_lsoa_var_quintile_sex_age=sum(n))


# pop_lsoa_2020_mapped_sex_age %>%
#   mutate(pop_lsoa_var_sex_age=as.numeric(pop_lsoa_var_sex_age)) %>%
#   filter(sex=="Female") %>%
#   ggplot(aes(x = age_group, y = pop_lsoa_var_sex_age)) +
#   geom_bar(stat = "identity")




```

```{r}


pop_lsoa_mapped_quintile_age = 
pop_lsoa_2020_mapped_quintile_age %>% mutate(YEAR="2020") %>% mutate(pop_lsoa_var_quintile_age=as.numeric(pop_lsoa_var_quintile_age)) %>%rbind(
pop_lsoa_2017_mapped_quintile_age %>% mutate(YEAR="2017") %>% mutate(pop_lsoa_var_quintile_age=as.numeric(pop_lsoa_var_quintile_age)))%>%rbind(
pop_lsoa_2011_mapped_quintile_age %>% mutate(YEAR="2011") %>% mutate(pop_lsoa_var_quintile_age=as.numeric(pop_lsoa_var_quintile_age)))%>%rbind(
pop_lsoa_2002_mapped_quintile_age %>% mutate(YEAR="2002") %>% mutate(pop_lsoa_var_quintile_age=as.numeric(pop_lsoa_var_quintile_age)))

pop_lsoa_mapped_quintile_sex_age = 
pop_lsoa_2020_mapped_quintile_sex_age %>% mutate(YEAR="2020") %>% mutate(pop_lsoa_var_quintile_sex_age=as.numeric(pop_lsoa_var_quintile_sex_age))%>%rbind(
pop_lsoa_2017_mapped_quintile_sex_age %>% mutate(YEAR="2017") %>% mutate(pop_lsoa_var_quintile_sex_age=as.numeric(pop_lsoa_var_quintile_sex_age)))%>%rbind(
pop_lsoa_2011_mapped_quintile_sex_age %>% mutate(YEAR="2011") %>% mutate(pop_lsoa_var_quintile_sex_age=as.numeric(pop_lsoa_var_quintile_sex_age)))%>%rbind(
pop_lsoa_2002_mapped_quintile_sex_age %>% mutate(YEAR="2002") %>% mutate(pop_lsoa_var_quintile_sex_age=as.numeric(pop_lsoa_var_quintile_sex_age)))


pop_lsoa_mapped_sex_age = 
pop_lsoa_2020_mapped_sex_age %>% mutate(YEAR="2020") %>% mutate(pop_lsoa_var_sex_age=as.numeric(pop_lsoa_var_sex_age))%>%rbind(
pop_lsoa_2017_mapped_sex_age %>% mutate(YEAR="2017") %>% mutate(pop_lsoa_var_sex_age=as.numeric(pop_lsoa_var_sex_age)))%>%rbind(
pop_lsoa_2011_mapped_sex_age %>% mutate(YEAR="2011") %>% mutate(pop_lsoa_var_sex_age=as.numeric(pop_lsoa_var_sex_age)))%>%rbind(
pop_lsoa_2002_mapped_sex_age %>% mutate(YEAR="2002") %>% mutate(pop_lsoa_var_sex_age=as.numeric(pop_lsoa_var_sex_age)))


pop_lsoa_mapped_quintile_age %>% write_rds("tmp/pop_lsoa_mapped_quintile_age.rds")
pop_lsoa_mapped_quintile_sex_age %>% write_rds("tmp/pop_lsoa_mapped_quintile_sex_age.rds")
pop_lsoa_mapped_sex_age %>% write_rds("tmp/pop_lsoa_mapped_sex_age.rds")


 
```



# Ethnicity Population Datasets

Will derive populations for the following groups:

Ethnicity and Age
Sex and Age
Ethnicity, Sex and Age

Note there are 3 different types of Ethnicity:

Ethnicity detailed and the high level groups that they map on to
Ethnicity high level only


```{r, join_pop_ests, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


ethnicity_population_mapping_2021 = data.frame(

project_detail = c(
"Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani",
"African",
"Any other Black background",
"Caribbean",
"Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean",
"Any other ethnic group",
"Any other ethnic group",
"Chinese",
"Any other White background",
"Any other White background",
"Any other White background",
"British",
"Irish"
),

ons_20b_detail = c(
"Asian, Asian British or Asian Welsh: Other Asian",
"Asian, Asian British or Asian Welsh: Bangladeshi",
"Asian, Asian British or Asian Welsh: Indian",
"Asian, Asian British or Asian Welsh: Pakistani",
"Black, Black British, Black Welsh, Caribbean or African: African",
"Black, Black British, Black Welsh, Caribbean or African: Other Black",
"Black, Black British, Black Welsh, Caribbean or African: Caribbean",
"Mixed or Multiple ethnic groups: Other Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups: White and Asian",
"Mixed or Multiple ethnic groups: White and Black African",
"Mixed or Multiple ethnic groups: White and Black Caribbean",
"Other ethnic group: Any other ethnic group",
"Other ethnic group: Arab",
"Asian, Asian British or Asian Welsh: Chinese",
"White: Gypsy or Irish Traveller",
"White: Roma",
"White: Other White",
"White: English, Welsh, Scottish, Northern Irish or British",
"White: Irish"
)

)



ethnicity_population_mapping_2011 = data.frame(

project_detail = c(
"Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani",
"African",
"Any other Black background",
"Caribbean",
"Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean",
"Any other ethnic group",
"Any other ethnic group",
"Chinese",
"Any other White background",
"Any other White background",
"British",
"Irish"
),

ons_2011_detail = c(

"Asian/Asian British: Other Asian",
"Asian/Asian British: Bangladeshi",
"Asian/Asian British: Indian",
"Asian/Asian British: Pakistani",
"Black/African/Caribbean/Black British: African",
"Black/African/Caribbean/Black British: Other Black",
"Black/African/Caribbean/Black British: Caribbean",
"Mixed/multiple ethnic group: Other Mixed",
"Mixed/multiple ethnic group: White and Asian",
"Mixed/multiple ethnic group: White and Black African",
"Mixed/multiple ethnic group: White and Black Caribbean",
"Other ethnic group: Any other ethnic group",
"Other ethnic group: Arab",
"Asian/Asian British: Chinese",
"White: Gypsy or Irish Traveller",
"White: Other White",
"White: English/Welsh/Scottish/Northern Irish/British",
"White: Irish"

)

)


ethnicity_population_mapping_2001 = data.frame(

project_detail = c(
"Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani",
"African",
"Any other Black background",
"Caribbean",
"Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean",
"Any other ethnic group",
"Chinese",
"Any other White background",
"British",
"Irish"
),

ons_2001_detail = c(
"Asian/Asian British: Other",
"Asian/Asian British: Bangladeshi",
"Asian/Asian British: Indian",
"Asian/Asian British: Pakistani",
"Black/Black British: Black African",
"Black/Black British: Other",
"Black/Black British: Black Caribbean",
"Mixed: Other",
"Mixed: White and Asian",
"Mixed: White and Black African",
"Mixed: White and Black Caribbean",
"Chinese/Other: Other",
"Chinese/Other: Chinese",
"White: Other",
"White: British",
"White: Irish"
)

)


ethnicity_population_mapping_high = data.frame(

project_detail = c(
"Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani",
"African",
"Any other Black background",
"Caribbean",
"Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean",
"Any other ethnic group",
"Chinese",
"Any other White background",
"British",
"Irish"
),

project_high = c(
"Asian or Asian British",
"Asian or Asian British",
"Asian or Asian British",
"Asian or Asian British",
"Black or Black British",
"Black or Black British",
"Black or Black British",
"Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups",
"Mixed or Multiple ethnic groups",
"Other ethnic groups",
"Other ethnic groups",
"White",
"White",
"White"
)

)


pop_ethnicity_2011%>%distinct(ethnicity)

```



```{r}


pop_ethnicity_2021 = 
pop_ethnicity_2021 %>%
  mutate(n=ifelse(n=="c",0,n)) %>%
  left_join(ethnicity_population_mapping_2021, by=c("ethnicity"="ons_20b_detail")) %>%
  mutate(ethnicity=project_detail) %>%
  select(ethnicity, sex, age, n) %>%
  arrange(ethnicity,sex,age) %>%
  mutate(n=as.numeric(n)) %>%
  group_by(ethnicity, sex, age) %>%
  summarise(n=sum(n)) %>%
  mutate(age=ifelse(age=="100 or over",as.numeric(101), as.numeric(age))) %>%
  ungroup() %>%
  filter(age>=18) %>%
  mutate(age_group =
          case_when(
age <18 ~ "<18",
(age >= 18 & age <= 25)~"18-24",
(age >= 25 & age <= 29)~ "25-29",
(age >= 30 & age <= 34)~ "30-34",
(age >= 35 & age <= 39)~ "35-39",
(age >= 40 & age <= 44)~ "40-44",
(age >= 45 & age <= 49)~ "45-49",
(age >= 50 & age <= 54)~ "50-54",
(age >= 55 & age <= 59)~ "55-59",
(age >= 60 & age <= 64)~ "60-64",
(age >= 65 & age <= 69)~ "65-69",
(age >= 70 & age <= 74)~ "70-74",
(age >= 75 & age <= 79)~ "75-79",
(age >= 80 & age <= 84)~ "80-84",
(age >= 85 & age <= 89)~ "85-89",
.default = ">=90"
)) %>%
  ungroup() %>%
  select(-age) %>%
  group_by(ethnicity, sex, age_group) %>%
  summarise(n=sum(n)) %>%
  ungroup() %>%
  rename(SEX=sex,ETHNIC_DESC_DETAIL2=ethnicity,pop_ethnicity_var_ethnicity_sex_age=n)




pop_ethnicity_2011 = 
pop_ethnicity_2011 %>%
  mutate(n=ifelse(n=="c",0,n)) %>%
  left_join(ethnicity_population_mapping_2011, by=c("ethnicity"="ons_2011_detail")) %>%
  filter(!is.na(detail_ethnicity)) %>%
  filter(high_level_ethnicity!="All categories") %>%
  select(-high_level_ethnicity,-detail_ethnicity) %>%
  mutate(ethnicity=project_detail) %>%
  select(ethnicity, sex, age_band, n) %>%
  arrange(ethnicity,sex,age_band) %>%
  mutate(n=as.numeric(n)) %>%
  group_by(ethnicity, sex, age_band) %>%
  summarise(n=sum(n)) %>%
  ungroup() %>%
  filter(!age_band%in%c("0-4","5-9","10-14","15-17")) %>%
  rename(SEX=sex,ETHNIC_DESC_DETAIL2=ethnicity,pop_ethnicity_var_ethnicity_sex_age=n,age_group=age_band)





pop_ethnicity_2001 = 
pop_ethnicity_2001 %>%
  mutate(n=ifelse(n=="c",0,n)) %>%
  left_join(ethnicity_population_mapping_2001, by=c("ethnicity"="ons_2001_detail")) %>%
  filter(!is.na(detail_ethnicity)) %>%
  filter(high_level_ethnicity!="All categories") %>%
  select(-high_level_ethnicity,-detail_ethnicity) %>%
  mutate(ethnicity=project_detail) %>%
  select(ethnicity, sex, age_band, n) %>%
  arrange(ethnicity,sex,age_band) %>%
  mutate(n=as.numeric(n)) %>%
  group_by(ethnicity, sex, age_band) %>%
  summarise(n=sum(n)) %>%
  ungroup() %>%
  filter(!age_band%in%c("0-4","5-9","10-14","15-17")) %>%
  rename(SEX=sex,ETHNIC_DESC_DETAIL2=ethnicity,pop_ethnicity_var_ethnicity_sex_age=n,age_group=age_band)
  




pop_ethnicity_2011 %>%
  mutate(n=ifelse(n=="c",0,n)) %>%
  left_join(ethnicity_population_mapping_2011, by=c("ethnicity"="ons_2011_detail")) %>%
  filter(!is.na(detail_ethnicity)) %>%
  filter(high_level_ethnicity!="All categories") %>%
  select(-high_level_ethnicity,-detail_ethnicity) %>%
  mutate(ethnicity=project_detail) %>%
  select(ethnicity, sex, age_band, n) %>%
  arrange(ethnicity,sex,age_band) %>%
  mutate(n=as.numeric(n)) %>%
  group_by(ethnicity, sex, age_band) %>%
  summarise(n=sum(n)) %>%
  ungroup() %>%
  filter(!age_band%in%c("0-4","5-9","10-14","15-17")) %>%
  rename(SEX=sex,ETHNIC_DESC_DETAIL2=ethnicity,pop_ethnicity_var_ethnicity_sex_age=n,age_group=age_band) %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>% distinct(ETHNIC_DESC_DETAIL2,project_high) %>%
  arrange(project_high,ETHNIC_DESC_DETAIL2)
  group_by(project_high, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_high_age=sum(pop_ethnicity_var_ethnicity_sex_age))

```





```{r}


# 2021

pop_ethnicity_2021_mapped_ethnicity_age = pop_ethnicity_2021 %>%
  group_by(ETHNIC_DESC_DETAIL2, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_age=sum(pop_ethnicity_var_ethnicity_sex_age))

pop_ethnicity_2021_mapped_sex_age = pop_ethnicity_2021 %>%
  group_by(SEX, age_group) %>%
  summarise(pop_ethnicity_var_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))

pop_ethnicity_2021_mapped_ethnicity_sex_age = pop_ethnicity_2021 %>%
  group_by(ETHNIC_DESC_DETAIL2, SEX, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))


pop_ethnicity_2021_mapped_ethnicity_high_age = pop_ethnicity_2021 %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>%
  group_by(project_high, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_high_age=sum(pop_ethnicity_var_ethnicity_sex_age))


pop_ethnicity_2021_mapped_ethnicity_high_sex_age = pop_ethnicity_2021 %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>%
  group_by(project_high, SEX, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_high_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))




# 2011

pop_ethnicity_2011_mapped_ethnicity_age = pop_ethnicity_2011 %>%
  group_by(ETHNIC_DESC_DETAIL2, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_age=sum(pop_ethnicity_var_ethnicity_sex_age))

pop_ethnicity_2011_mapped_sex_age = pop_ethnicity_2011 %>%
  group_by(SEX, age_group) %>%
  summarise(pop_ethnicity_var_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))

pop_ethnicity_2011_mapped_ethnicity_sex_age = pop_ethnicity_2011 %>%
  group_by(ETHNIC_DESC_DETAIL2, SEX, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))


pop_ethnicity_2011_mapped_ethnicity_high_age = pop_ethnicity_2011 %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>%
  group_by(project_high, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_high_age=sum(pop_ethnicity_var_ethnicity_sex_age))


pop_ethnicity_2011_mapped_ethnicity_high_sex_age = pop_ethnicity_2011 %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>%
  group_by(project_high, SEX, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_high_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))



```



```{r}


# 2001

pop_ethnicity_2001_mapped_ethnicity_age = pop_ethnicity_2001 %>%
  group_by(ETHNIC_DESC_DETAIL2, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_age=sum(pop_ethnicity_var_ethnicity_sex_age))

pop_ethnicity_2001_mapped_sex_age = pop_ethnicity_2001 %>%
  group_by(SEX, age_group) %>%
  summarise(pop_ethnicity_var_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))

pop_ethnicity_2001_mapped_ethnicity_sex_age = pop_ethnicity_2001 %>%
  group_by(ETHNIC_DESC_DETAIL2, SEX, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))


pop_ethnicity_2001_mapped_ethnicity_high_age = pop_ethnicity_2001 %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>%
  group_by(project_high, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_high_age=sum(pop_ethnicity_var_ethnicity_sex_age))


pop_ethnicity_2001_mapped_ethnicity_high_sex_age = pop_ethnicity_2001 %>%
  left_join(ethnicity_population_mapping_high,
  by=c("ETHNIC_DESC_DETAIL2"="project_detail")) %>%
  group_by(project_high, SEX, age_group) %>%
  summarise(pop_ethnicity_var_ethnicity_high_sex_age=sum(pop_ethnicity_var_ethnicity_sex_age))




```



```{r}


pop_ethnicity_mapped_ethnicity_age = 
pop_ethnicity_2021_mapped_ethnicity_age %>% mutate(YEAR="2021")%>%rbind(
pop_ethnicity_2011_mapped_ethnicity_age %>% mutate(YEAR="2011"))%>%rbind(
pop_ethnicity_2001_mapped_ethnicity_age %>% mutate(YEAR="2001"))


pop_ethnicity_mapped_sex_age = 
pop_ethnicity_2021_mapped_sex_age %>% mutate(YEAR="2021")%>%rbind(
pop_ethnicity_2011_mapped_sex_age %>% mutate(YEAR="2011"))%>%rbind(
pop_ethnicity_2001_mapped_sex_age %>% mutate(YEAR="2001"))


pop_ethnicity_mapped_ethnicity_sex_age = 
pop_ethnicity_2021_mapped_ethnicity_sex_age %>% mutate(YEAR="2021")%>%rbind(
pop_ethnicity_2011_mapped_ethnicity_sex_age %>% mutate(YEAR="2011"))%>%rbind(
pop_ethnicity_2001_mapped_ethnicity_sex_age %>% mutate(YEAR="2001"))

pop_ethnicity_mapped_ethnicity_high_age = 
pop_ethnicity_2021_mapped_ethnicity_high_age %>% mutate(YEAR="2021")%>%rbind(
pop_ethnicity_2011_mapped_ethnicity_high_age %>% mutate(YEAR="2011"))%>%rbind(
pop_ethnicity_2001_mapped_ethnicity_high_age %>% mutate(YEAR="2001"))

pop_ethnicity_mapped_ethnicity_high_sex_age = 
pop_ethnicity_2021_mapped_ethnicity_high_sex_age %>% mutate(YEAR="2021")%>%rbind(
pop_ethnicity_2011_mapped_ethnicity_high_sex_age %>% mutate(YEAR="2011"))%>%rbind(
pop_ethnicity_2001_mapped_ethnicity_high_sex_age %>% mutate(YEAR="2001"))



pop_ethnicity_mapped_ethnicity_age %>% write_rds("tmp/pop_ethnicity_mapped_ethnicity_age.rds")
pop_ethnicity_mapped_sex_age %>% write_rds("tmp/pop_ethnicity_mapped_sex_age.rds")
pop_ethnicity_mapped_ethnicity_sex_age %>% write_rds("tmp/pop_ethnicity_mapped_ethnicity_sex_age.rds")
pop_ethnicity_mapped_ethnicity_high_age %>% write_rds("tmp/pop_ethnicity_mapped_ethnicity_high_age.rds")
pop_ethnicity_mapped_ethnicity_high_sex_age %>% write_rds("tmp/pop_ethnicity_mapped_ethnicity_high_sex_age.rds")





# Overall Population


pop_ethnicity_2011 = dbGetQuery(con,sprintf(glue(select_all_proj,"population_estimates_2011_ethnicity_age_sex")))

ethnicity_population_mapping_2011 = data.frame(

project_detail = c(
"Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani",
"African",
"Any other Black background",
"Caribbean",
"Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean",
"Any other ethnic group",
"Any other ethnic group",
"Chinese",
"Any other White background",
"Any other White background",
"British",
"Irish"
),

ons_2011_detail = c(

"Asian/Asian British: Other Asian",
"Asian/Asian British: Bangladeshi",
"Asian/Asian British: Indian",
"Asian/Asian British: Pakistani",
"Black/African/Caribbean/Black British: African",
"Black/African/Caribbean/Black British: Other Black",
"Black/African/Caribbean/Black British: Caribbean",
"Mixed/multiple ethnic group: Other Mixed",
"Mixed/multiple ethnic group: White and Asian",
"Mixed/multiple ethnic group: White and Black African",
"Mixed/multiple ethnic group: White and Black Caribbean",
"Other ethnic group: Any other ethnic group",
"Other ethnic group: Arab",
"Asian/Asian British: Chinese",
"White: Gypsy or Irish Traveller",
"White: Other White",
"White: English/Welsh/Scottish/Northern Irish/British",
"White: Irish"

)

)




pop_ethnicity_2011 = 
pop_ethnicity_2011 %>%
  mutate(n=ifelse(n=="c",0,n)) %>%
  left_join(ethnicity_population_mapping_2011, by=c("ethnicity"="ons_2011_detail")) %>%
  filter(!is.na(detail_ethnicity)) %>%
  filter(high_level_ethnicity!="All categories") %>%
  select(-high_level_ethnicity,-detail_ethnicity) %>%
  mutate(ethnicity=project_detail) %>%
  select(ethnicity, sex, age_band, n) %>%
  arrange(ethnicity,sex,age_band) %>%
  mutate(n=as.numeric(n)) %>%
  group_by(ethnicity, age_band) %>%
  summarise(n=sum(n)) %>%
  ungroup() %>%
  filter(!age_band%in%c("0-4","5-9","10-14","15-17")) %>%
  rename(ETHNIC_DESC_DETAIL2=ethnicity,pop_ethnicity_var_ethnicity_overall_age=n,age_group=age_band)

pop_ethnicity_2011_mapped_overall_age = pop_ethnicity_2011 %>%
  group_by(age_group) %>%
  summarise(pop_ethnicity_var_overall_age=sum(pop_ethnicity_var_ethnicity_overall_age))


pop_ethnicity_mapped_ethnicity_overall_age = 
pop_ethnicity_2011_mapped_overall_age

```






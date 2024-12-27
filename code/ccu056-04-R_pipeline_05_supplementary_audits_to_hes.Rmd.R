---
title: "CCU056 Pipeline"
subtitle: ""
output:
  bookdown::html_document2:
    number_sections: true
css: style.css
---


WIP - Audits to HES exploration

E.g. how many people found in Audits that we didnt find in HES


```{r, setup, warning=FALSE, error=FALSE, message=FALSE,eval=TRUE, echo=FALSE}


#libraries
library(odbc)
library(DBI)
library(tidyverse)
library(glue)

#connect
con <- DBI::dbConnect(odbc:::odbc(), "databricks")

#parameters
dsa = "dsa_391419_j3w9t_collab"
proj = "ccu056"
select_all = glue("SELECT * FROM ",dsa,".")
select_all_proj = glue("SELECT * FROM ",dsa,".",proj,"_")


```


```{r, data, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

main_cohort_final_first = read_rds("tmp/main_cohort_final_first_new2.rds")

outcomes_hospitalisations <- dbGetQuery(con,sprintf(glue(select_all_proj,"outcomes_hospitalisations")))
outcomes_deaths <- dbGetQuery(con,sprintf(glue(select_all_proj,"outcomes_deaths")))


covariates = dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_covariates_1st_wide_icd_only")))

covariates_codelist = dbGetQuery(con,sprintf(glue(select_all_proj,"out_codelist_covariates")))


```

```{r, parameters, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

tavi_colour = "#ff0030"
savr_colour = "#b5e3eb"


```



```{r}

# seeing how LSOAs change if mapping to most appropriate year

lsoa_2011_imd = dbGetQuery(con,sprintf(glue(select_all,"hds_cur_lsoa_2011_imd_lookup")))

compare_df = main_cohort_final_first %>%
  select(PERSON_ID,LSOA,DECILE,QUINTILE) %>%
  left_join(lsoa_2011_imd,by=c("LSOA"="LSOA_2011")) %>%
  filter(DECILE!="Unknown") %>%
  filter(!is.na(IMD_2019_DECILES))
  

compare_df %>%
  filter(IMD_2019_QUINTILES!=QUINTILE)



```


# Audits to HES


**NACSA SAVR**

We found 72,705 unique events in HES APC (SAVR procedures) of which 54,240 were found in NACSA (SAVR procedures) within 7 days. Thus, 18,465 cases identified in the HES APC were not found in NACSA within 7 days.

How many in the NACSA SAVR cohort were found in HES APC?

Note that there is a one-to-many relationship between the HES APC cohort and NACSA SAVR. This is caused by the audit not being cleaned and in some instances an event in HES APC maps to more than one event in the audit (within 7 days). This means that the 54,240 events found in the HES APC actually account for 54,255 events in NACSA. The NACSA audit however has 63,295 events thus 9,040 events found in NACSA are not in our HES APC cohort (within 7 days). Note that it is likely to be less than 9,040 events as the NACSA audit has not been cleaned and had any washout period applied, thus there may be cases where washout events are being counted as events but should be excluded.


```{r, vennSAVR_working, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

hes_savr_distinct_in_audit = hes_cohort_savr %>%
  left_join((nacsa_prepared_savr%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)%>%distinct()),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_7 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_7 = ifelse(DATE_7<0,DATE_7*-1,DATE_7)) %>%
  mutate(bin=ifelse(DATE_7<=7,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  #select(PERSON_ID, bin) %>%
  distinct() %>%
  filter(bin==1) %>%
  select(-bin) %>%
  select(PERSON_ID,DATE_AND_TIME_OF_OPERATION) %>%
  distinct()


savr_audit_distinct = nacsa_prepared_savr %>%
  select(PERSON_ID,DATE_AND_TIME_OF_OPERATION) %>%
  distinct()


setdiff(savr_audit_distinct,hes_savr_distinct_in_audit)

# post 2018 only to align with TAVI
hes_savr_distinct_in_audit %>% filter(DATE_AND_TIME_OF_OPERATION >= "2018-01-01")

```


```{r, vennSAVR, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=5, fig.cap="SAVR HES APC and Audit Intersection"}

in_HES_only <- 18465
in_NACSA_only <- 9040
in_both <- 54255



ggplot() +
  xlim(0, 10) +
  ylim(0, 10) +
  coord_fixed(ratio = 1)  +

  ggforce::geom_circle(aes(x0 = 4, y0 = 5, r = 3), color = savr_colour, fill = NA) +
  ggforce::geom_circle(aes(x0 = 6, y0 = 5, r = 3), color = "#0D78E8", fill = NA) +
  geom_text(aes(x = 0, y = 5), label = "HES APC", color = savr_colour) +
  geom_text(aes(x = 9, y = 5), label = "NACSA SAVR", color = "#0D78E8", hjust=-0.1) +
  
  geom_text(aes(x = 1.5, y = 5), label = "18,465", color = "black", hjust=-0.1) +
  geom_text(aes(x = 7.8, y = 5), label = "9,040", color = "black", hjust=-0.1) +
  geom_text(aes(x = 4.8, y = 5), label = "54,255", color = "black", hjust=-0.1) +
  #geom_text(aes(x = 5, y = 5), label = "HES APC\n ∩ \nNACSA SAVR", color = "black") +
  theme_void() +
  theme(legend.position = "none") +
  
  coord_cartesian(xlim = c(-2,13),clip = "off")


```


**TAVI**

We found 23,725 unique events in HES APC (TAVI procedures) of which 20,160 were found in TAVI audit within 28 days. Thus, 3,565 cases identified in the HES APC were not found in TAVI within 28 days.

How many in the TAVI cohort were found in HES APC?

Note that there is a one-to-many relationship between the HES APC cohort and TAVI. This is caused by the audit not being cleaned and in some instances an event in HES APC maps to more than one event in the audit (within 28 days). This means that the 20,160 events found in the HES APC actually account for 20,210 events in TAVI. The TAVI audit however has 24,855 events thus 4,645 events found in TAVI are not in our HES APC cohort (within 28 days).



```{r, vennTAVI_working, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

hes_tavi_distinct_in_audit = hes_cohort_tavi %>%
  left_join((tavi_prepared%>%select(PERSON_ID,DATE_AND_TIME_OF_OPERATION)%>%distinct()),
            by = join_by('PERSON_ID')) %>%
  mutate(DATE_28 = as.numeric(DATE_AND_TIME_OF_OPERATION-OPERATION_DATE)) %>%
  mutate(DATE_28 = ifelse(DATE_28<0,DATE_28*-1,DATE_28)) %>%
  mutate(bin=ifelse(DATE_28<=28,1,0)) %>%
  mutate(bin=ifelse(is.na(bin),0,bin)) %>%
  #select(PERSON_ID, bin) %>%
  distinct() %>%
  filter(bin==1) %>%
  select(-bin) %>%
  select(PERSON_ID,DATE_AND_TIME_OF_OPERATION) %>%
  distinct()


tavi_audit_distinct = tavi_prepared %>%
  select(PERSON_ID,DATE_AND_TIME_OF_OPERATION) %>%
  distinct()


setdiff(tavi_audit_distinct,hes_tavi_distinct_in_audit)


```


```{r, vennTAVI, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=5, fig.cap="TAVI HES APC and Audit Intersection"}

in_HES_only <- 3565
in_TAVI_only <- 4645
in_both <- 20210



ggplot() +
  xlim(0, 10) +
  ylim(0, 10) +
  coord_fixed(ratio = 1)  +

  ggforce::geom_circle(aes(x0 = 4, y0 = 5, r = 3), color = tavi_colour, fill = NA) +
  ggforce::geom_circle(aes(x0 = 6, y0 = 5, r = 3), color = "#5D0505", fill = NA) +
  geom_text(aes(x = 0, y = 5), label = "HES APC", color = tavi_colour) +
  geom_text(aes(x = 9, y = 5), label = "TAVI", color = "#5D0505", hjust=-1) +
  
  geom_text(aes(x = 1.5, y = 5), label = "3,565", color = "black", hjust=-0.1) +
  geom_text(aes(x = 7.8, y = 5), label = "4,656", color = "black", hjust=-0.1) +
  geom_text(aes(x = 4.8, y = 5), label = "20,210", color = "black", hjust=-0.1) +
  #geom_text(aes(x = 5, y = 5), label = "HES APC\n ∩ \nTAVI", color = "black") +
  theme_void() +
  theme(legend.position = "none") +
  
  coord_cartesian(xlim = c(-2,13),clip = "off")


```





## Supplementary Cohort

Assuming that we require a person to be in the full cohort to be included in the supplementary cohort:

* approx (54,255 + 20,210) 74,465 if we include NACSA and TAVI from the beginning of their coverage (2013 and 2018 respectively)
* approx (23,280 + 20,210) 43,490 if we only include NACSA and TAVI from 2018



---
title: "CCU056 Pipeline"
subtitle: "Creating the cohort - 01"
output:
  bookdown::html_document2:
    number_sections: true
css: style.css
---

# Introduction

We here curate the initial cohort: patients who have underwent TAVI/SAVR for AS.

Note that the initial part of this pipeline was curated using PySpark and these notebooks can be found in Databricks.

In order to maximise coverage, a cohort derived using HES APC instead of NICOR Audits will be used for the main analysis of this project.


## Project aims and reserach questions

**The aims of project CCU056 are as follows:**

i. describe and compare the characteristics of patients treated for AS by sex, ethnicity and socioeconomic status; and
ii. see if the profile of patients treated for AS differs between pre and during Covid-19 pandemic time-periods.

**The research questions are as follows:**

1. What is the socio-demographic make-up of patients undergoing aortic valve intervention (SAVR/TAVI) in England, and has this changed over the last 20 years (or as long as available dataset allows)?  
2. Has the socio-demographic make-up of patients undergoing SAVR/TAVI in England prior to 2020 changed during the Covid-19 pandemic?  
3. What has been the impact of Covid-19 on overall outcomes related to SAVR/TAVI? 
4.  Are there any differences in outcomes following SAVR/TAVI between different socio-demographic subgroups?



# Project Parameters

<br>
<span style="color:#A95EF4;">**Study Start Date: **</span>Procedures must occur >= 2000-01-01

<span style="color:#A95EF4;">**Study End Date: **</span>2023-12-01

<br>
<span style="color:#A95EF4;">**Aortic Stenosis Selection Groups:**</span>

Where x is the difference in years from the first SAVR/TAVI date of procedure to the AS diagnosis date, the priority groups are defined as follows:

**1.** -infinity <= x < 0 (**all years before**)<br>
**2.** 0 <= x < +0.5 (**half a year after**)

For repeat procedures this is amended to account for the procedures that occurred prior. 

That is, for priority group 1, the AS diagnosis must be found more than half a year after the previous procedure.


<span style="color:#A95EF4;">**SAVR/TAVI washout periods:**</span>

28 days (4 weeks) - i.e. a code found within 4 weeks of the first code would be considered to be the same procedure. Codes found after 4 weeks would be counted as a re-do procedure.



<span style="color:#A95EF4;">**COVID Treatment Group Time Periods:**</span>

Time periods are split into prior and during COVID groups as follows:

* PRIOR groups: Beginning 2000-2004, End 2015-2019
* PANDEMIC groups: During 2020-latest

For cohort purposes we have allocated a third PRIOR group called Middle 2005-2014 to ensure that every person is allocated a time period.



<span style="color:#A95EF4;">**Inclusions:**</span>

* Patients above the age of 18 years
* Patients that underwent SAVR for AS (with or without coronary artery bypass/aortic surgery) OR
* Patients that underwent TAVI for aortic stenosis


<span style="color:#A95EF4;">**Exclusions:**</span>

* SAVR or TAVI for any other reason except aortic stenosis as the predominant lesion
* Previous SAVR/TAVI, prior to the study time-window.


## Inclusion Codelists

Inclusion codelists supplied by Anvesha for Aortic Stenosis and SAVR/TAVI procedures.


### Aortic Stenosis


| ICD10 Code | Definition |
| :-- | :-- |
| I06.0	| Rheumatic aortic stenosis |
| I06.2	| Rheumatic aortic stenosis with insufficiency |
| I35.0	| Nonrheumatic aortic (valve) stenosis |
| I35.2	| Aortic (valve) stenosis with insufficiency |
| Q23.0	| Congenital stenosis of aortic valve |



### SAVR

OPCS-4.10 codes for Aortic Valve Replacement<br>
https://classbrowser.nhs.uk/#/book/OPCS-4.10/volume1-p2-4.html+K26.4

Note that K265 to K269 are repair codes.<br>


| OPCS4 Code | Definition |
| :-- | :-- |
| K26	| Plastic repair of aortic valves |
| K261	| Allograft replacement of aortic valve |
| K262	| Xenograft replacement of aortic valve |
| K263	| Prosthetic replacement of aortic valve |
| K264	| Replacement of aortic valve NEC |
| K265	| Aortic valve repair NEC Includes: Aortic valvuloplasty NEC |
| K268	| Other specifiedy |
| K269	| Unspecified |


### TAVI

https://classbrowser.nhs.uk/ref_books/OPCS-4.9_NCCS-2021.pdf

| OPCS4 Code | Definition |
| :-- | :-- |
| PCSK1	| Transcatheter aortic valve implantation (K26) |

Using the classbrowser above there are 2 TAVI code combinations:
    
For transcatheter aortic valve implantation (TAVI) using a **surgical approach** through left
ventricle (transapical or transventricular approach) the following codes must be assigned:

* K26.- Plastic repair of aortic valve<br>
* Y49.4 - Transapical approach to heart<br>
* Y53 - Approach to organ under image control **or**
  Y68.- Other approach to organ under image control
  
For TAVI using a **transluminal approach** through an artery (i.e. femoral, subclavian, axillary
or aorta) the following codes must be assigned:

* K26.- Plastic repair of aortic valve<br>
* Y79.- Approach to organ through artery<br>
* Y53.- Approach to organ under image control **or**
 Y68.- Other approach to organ under image control

 **TAVI will be defined using the above 2 approaches. That is, we will use the surgical approach TAVI definition and the transluminal approach TAVI definition.**

TAVI combinations will be identified and named as follows:

 **TAVI 1** - K26 & Y49 & Y53 (surgical)<br>
 **TAVI 2** - K26 & Y49 & Y68 (surgical)<br>
 **TAVI 3** - K26 & Y79 & Y53 (transluminal)<br>
 **TAVI 4** - K26 & Y79 & Y68 (transluminal)
 
 
 **Note that K26 is included as a code to define SAVR but also included in all TAVI combinations. Thus, SAVR cases will exclude any cases identified as a TAVI case.**

```{r, setup, warning=FALSE, error=FALSE, message=FALSE,eval=TRUE, echo=FALSE}

#all_times

#libraries
library(odbc)
library(DBI)
library(tidyverse)
library(glue)

#connect
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


```{r, data, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# Parameters of pipeline
parameters_archived_on <- dbGetQuery(con,sprintf(glue(select_all_proj,"parameters_df_datasets")))
parameters_archived_on %>% write_rds("tmp/parameters_archived_on.rds")

# HES APC filtered for AS codes and procedure codes
as_codes <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_cases_as_codes")))
procedure_codes <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_cases_procedure_codes")))
# Operation dates from HES APC OTR
procedure_codes_op_dates <- dbGetQuery(con,sprintf(glue(select_all_proj,
                                                        "tmp_cases_procedure_codes_operation_dates")))

# AS, SAVR and TAVI codelists
codelists_inclusions <- dbGetQuery(con,sprintf(glue(select_all_proj,"out_codelists_inclusions")))


# Been redone in Databricks
#as_codes = as_codes %>% filter(!CODE %in% c("I068","I069","I080","I358","I359","I391"))



```

```{r, lookup_tables, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

icd10 <- dbGetQuery(con,"SELECT * FROM dss_corporate.icd10_group_chapter_v01")
opcs = dbGetQuery(con,"SELECT * FROM dss_corporate.opcs_codes_v02")
gdppr_ref = dbGetQuery(con,"SELECT * FROM dss_corporate.gdppr_cluster_refset")


```

```{r, parameters, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

study_start_date = "2020-01-01"

tavi_colour = "#ff0030"
savr_colour = "#b5e3eb"


```


# Pipeline Parameters

```{r, parametersArchivedOn, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

parameters_archived_on = read_rds("tmp/parameters_archived_on.rds")

archived_on = parameters_archived_on %>%
  select(dataset,archived_on) %>%
  filter(dataset %in% c("hes_apc", "hes_apc_otr", "nacsa", "tavi", "deaths", "gdppr")) %>%
  mutate(dataset=case_when(
    dataset =="hes_apc" ~ "HES APC",
    dataset =="hes_apc_otr" ~ "HES APC Other",
    dataset =="nacsa" ~ "NICOR NACSA",
    dataset =="tavi" ~ "NICOR TAVI",
    dataset =="deaths" ~ "Deaths",
    dataset =="gdppr" ~ "GDPPR"
  )) %>%
  rename(Dataset=dataset, `Archived On`=archived_on)


kableExtra::kbl(
  archived_on,
    booktabs = T, 
    caption=("Datasets used to prepare this exploration dataset"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

```

# Aortic Stenosis pathology

Note that we apply no study start date cut-off to this cohort as AS selection priority group 1 covers all years prior to treatment.

* As seen in Table \@ref(tab:asCodesCount), ICD10 codes I350 and I080 drive AS diagnosis, the latter of which may be removed.

* Choosing the narrow phenotype would remove 23.3% codes.

* Figure \@ref(fig:asCoveragePlot) shows that there is sufficient coverage (~1995 onwards) before the study start date to allow for SAVR/TAVI cases found at the beginning of the study (`r study_start_date`) to be included in the cohort if an AS code can be found in the years prior.


```{r, as_codes_count_working, warning=FALSE, message=FALSE, error=FALSE, eval=FALSE, echo=FALSE}


as_codes_coverage = as_codes %>%
  mutate(AS = ifelse(CODE %in% c("I068","I069","I358","I359","I391", "I080"),0,1)) %>%
  #find unique values by person only - excluding EPISTART
  select(PERSON_ID,CODE,EPISTART,AS) %>%
  distinct() %>%
  group_by(CODE,AS) %>%
  count() %>% 
  ungroup() %>%
  mutate(PCT = round((n/sum(n))*100,1)) %>%
  left_join((codelists_inclusions%>%select(code,term)%>%distinct()),by=c("CODE"="code"))


as_codes_coverage %>% write_rds("tmp/as_codes_coverage.rds")


# how many codes removed if we go with narrow definition?
as_codes_coverage %>% filter(AS==0) %>% summarise(n=sum(n),PCT=sum(PCT))


```


```{r, asCodesCount, warning=FALSE, message=FALSE, error=FALSE, eval=TRUE, echo=FALSE}

as_codes_coverage = read_rds("tmp/as_codes_coverage.rds")


kableExtra::kbl(
  (as_codes_coverage%>%select(-AS) %>%
     bind_rows(as_codes_coverage %>%
                 summarise(n = sum(n, na.rm = TRUE)) %>%
                 mutate(PCT=100.0) %>%
                 mutate(CODE="",term="")) %>%
     #apply SDC
     mutate(n = case_when(
     n < 10 ~ 10,
     TRUE ~ round(n / 5) * 5
     )) %>%
     mutate(n=format(n, big.mark = ","))%>%
     relocate(term, .after=CODE)%>%
     rename(N=n,`CODE DESCRIPTION`=term)
     ),
    booktabs = T, 
    caption=("Aortic Stenosis codes that are driving cohort: Distinct PERSON + CODE combinations <br> SDC applied"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )


```


```{r, as_coverage_working, warning=FALSE, message=FALSE, error=FALSE, eval=FALSE, echo=FALSE}

count_data = as_codes %>%
  select(PERSON_ID,EPISTART) %>%
  filter(EPISTART>"1994-01-01") %>%
  mutate(EPISTART_MONTH = month(EPISTART),
         EPISTART_YEAR = year(EPISTART)) %>%
  mutate(year_month = paste(EPISTART_YEAR, EPISTART_MONTH, sep = "-")) %>%
  group_by(PERSON_ID,year_month,EPISTART_MONTH,EPISTART_YEAR) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))

count_data_min = count_data%>%ungroup()%>%summarize(min_date = min(year_month_date))%>%pull()
count_data_max = count_data%>%ungroup()%>%summarize(max_date = max(year_month_date))%>%pull()

full_date_df = data.frame(year_month_date = seq(
  count_data_min,
  count_data_max,
  by = "1 month")) %>%
  left_join((count_data%>%ungroup()%>%select(-PERSON_ID)%>%mutate(distinct=1))) %>%
  mutate(count = coalesce(count, 0),
         distinct = coalesce(distinct, 0)) %>%
  mutate(EPISTART_MONTH = month(year_month_date)) %>%
  mutate(EPISTART_YEAR = year(year_month_date)) %>%
  mutate(year_month = paste(EPISTART_YEAR, EPISTART_MONTH, sep = "-")) %>%
  group_by(year_month) %>%
  summarise(count=sum(count),distinct=sum(distinct)) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  pivot_longer(cols=c(count,distinct),names_to="group") %>%
  mutate(group=ifelse(group=="count","Records with valid IDs", "Distinct IDs")) %>%
  mutate(group = factor(group, levels = c("Records with valid IDs","Distinct IDs"))) %>%
  ungroup()

full_date_df %>% write_rds("tmp/as_coverage_data.rds")


```


```{r, asCoveragePlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="Aortic Stenosis Coverage in HES APC"}

full_date_df = read_rds("tmp/as_coverage_data.rds")

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))


label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month,year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 

ggplot(full_date_df, aes(x = year_month_label, y = value, group = group, color=group)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt")),
    legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt")
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("Records with valid IDs" = "#FD7E0D", "Distinct IDs" = "#36A333")) +
  geom_vline(xintercept = "2000-01", linetype = "dashed", color = "gray70") +
  geom_text(aes(x = "2000-01", y = 8000, label = "Study start date"), hjust = -0.05, color = "gray70") 




```


# Main Procedure Cohort

We now derive the main cohort using HES APC for SAVR and TAVI procedures. 


## Case Methodology

We define a case using `date of operation` obtained from HES APC Other instead of `EPISTART` or `ADMIDATE` from HES APC.


```{r, procedure_working, warning=FALSE, message=FALSE, error=FALSE, eval=FALSE, echo=FALSE}

# same amount of rows as procedure codes
procedure_codes_joined = procedure_codes %>%
  left_join((procedure_codes_op_dates%>%distinct), by=c("PERSON_ID","EPIKEY","CODE", "OPERTN_POSITION"="POSITION"))

rm(procedure_codes_op_dates)
rm(procedure_codes)




procedure_codes = 
procedure_codes_joined %>%
  mutate(op_date_type = ifelse(is.na(OPDATE),0,1)) %>%
  mutate(OPERATION_DATE = ifelse(is.na(OPDATE),EPISTART,OPDATE)) %>%
  select(-OPDATE)

rm(procedure_codes_joined)


procedure_codes = procedure_codes %>%
  mutate(OPERATION_DATE=as.Date(OPERATION_DATE, origin = "1970-01-01"))


op_date_types = procedure_codes %>% count(op_date_type) %>% mutate(pct=round(n/sum(n)*100,3)) %>%
  mutate(op_date_type=ifelse(op_date_type==0,"EPISTART","OPERATION DATE"))

op_date_types %>% write_rds("tmp/op_date_types.rds")
  
```

```{r, epistartPct, warning=FALSE, message=FALSE, error=FALSE, eval=TRUE, echo=FALSE}


op_date_types = read_rds("tmp/op_date_types.rds")
ep_dates_pct = op_date_types %>% filter(op_date_type=="EPISTART") %>% select(pct) %>% pull()

```


Note that in `r ep_dates_pct`% of records, `date of operation` was null and in these cases `EPISTART` has been used as a substitute (rather than removed from the cohort).


We begin by defining TAVI cases as SAVR and TAVI case intersections will be removed from the SAVR cohort.

**TAVI case definition:**

TAVI cases need to have all 3 of the required codes present within the 28 day (4 week) washout period.

The process, for each TAVI combination, is as follows:

1. We will find all records of the codes that make up the TAVI combination in question, e.g. for TAVI 1 we will find all occurrences of ICD10 codes K26, Y49, Y53 in HES APC.
2. By person, we will then find the earliest record in which one of the above codes is recorded. This will be set at Day 0.
3. We now begin an iteration process, starting at Day 0 and thereafter working through each date in which someone has one of these records above, to find out if a person has all 3 of the codes recorded at some point between Day 0 and Day 28, e.g. for the example above, are all 3 of the ICD10 codes found as such 0 ≤ K26, Y49, Y53 < 28.

The iteration process to find the first case has been coded as follows:

**Iteration 1.** We identify the code that was found at Day 0 and for the remaining 2 codes we find the earliest record of these that occur after Day 0, e.g. if the earliest record for a person was found as at 2020-01-01 and this was for code Y49 then we set this equal to Code A at Day 0 and then find the dates of the first records for codes K26 and Y53. If these records fall within the washout period then we have identified the persons first instance of TAVI and set their date of procedure equal to Day 0, here 2020-01-01. If codes K26 and Y53 cannot be found within the washout period then we discard Code A and move to iteration 2.
<br>**Iteration 2.** We again find the earliest record but this time the earliest record that exists after Day 0. This could again be Code A or a new code, say Code B or Code C. Say for this example the next earliest record found is for the code K26 as at 2020-01-09. The washout period resets and Day 0 is now set equal to Code B. The previous earliest record, Code A, is discarded. We now find the earliest records that occur for Code A and Code C after Day 0. If these records fall within the washout period then we have identified the persons first instance of TAVI and set their date of procedure equal to Day 0, here 2020-01-09. If codes Y49 and Y53 cannot be found within the washout period then we discard Code B, that occurs at 2020-01-09, now too and move to the next iteration.
<br>**Iteration X.** We repeat this iterative process until we find the first instance of TAVI.

**Repeat procedures:**

Once we have found the first TAVI case, we continue the iterative process for every subsequent date. We then have a set of TAVI cases defined for each person; however, there may be washout period overlaps between these cases, e.g. a TAVI case was detected as at iteration 2 and iteration 3 but the date of procedure for iteration 3 occurs within 28 days of the date of procedure of iteration 2. In this case the TAVI procedure identified at iteration 3 would be discarded.

A example is diagrammed below: 


```{r, test, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=2}


df = data.frame(Start_date=c(0,28),
           End_date=c(28,40),
           variable=c("washout","post"),
           value=c(1,1)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 5) +
  #scale_y_continuous(limits = c(1,10)) +
  theme(
    plot.margin = margin(10,30,62,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    axis.text.y = element_blank(),   # Remove y-axis labels
    axis.ticks.y = element_blank(),
    plot.title.position = "plot"
    ) +
  labs(title = "Iteration 1") +
  scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(0, 7, 14, 21, 28)) +
     coord_cartesian(xlim = c(1, 40),ylim = c(1,3),clip = "off") +
  scale_color_manual(values = c("washout" = "#D168CF", "post" = "gray70")) +
  geom_text(aes(x = 15, y = 2.5, label = "Washout period"), color = "#D168CF",size=5) +
  geom_text(aes(x = 41.5, y = -1, label = "Days"), color = "black") +
  #annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 28, xend = 28, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
    annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  geom_rect(aes(xmin = -1, xmax = 1, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = -1, y = 0, label = "Code A"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 7.7, xmax = 9.8, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = 7.7, y = 0, label = "Code B"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 16.7, xmax = 18.8, ymin = -1, ymax = 0), color= "gray70", fill="white",alpha = 0.5,linetype=2) +
  geom_text(aes(x = 16.7, y = 0, label = "Code B"), color = "black", size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 29.2, xmax = 31.3, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = 29.2, y = 0, label = "Code C"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 31.7, xmax = 33.7, ymin = -1, ymax = 0), color= "gray70", fill="white",alpha = 0.5,linetype=2) +
  geom_text(aes(x = 31.7, y = 0, label = "Code A"), color = "black", size=3, vjust=1.7, hjust=-0.16) +
  
  geom_segment(aes(x= 10, y=-2,xend=28, yend=-2), 
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 10, y=-2,xend=0, yend=-2), 
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_text(aes(x = 15, y = -2.1, label = "No TAVI case identified"), color = "black", size=5, vjust=1.7, hjust=0.51)
  





```

```{r, test1, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=2}

df = data.frame(Start_date=c(-10,0,28),
           End_date=c(0,28,35),
           variable=c("pre","washout","post"),
           value=c(1,1,1)
           )

#0,10,14,31,36

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 5) +
  #scale_y_continuous(limits = c(1,10)) +
  theme(
    plot.margin = margin(10,30,62,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    axis.text.y = element_blank(),   # Remove y-axis labels
    axis.ticks.y = element_blank(),
    plot.title.position = "plot"
    ) +
  labs(title = "Iteration 2") +
  scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(0, 7, 14, 21, 28)) +
     coord_cartesian(xlim = c(-8.5, 35),ylim = c(1,3),clip = "off") +
  scale_color_manual(values = c("washout" = "#D168CF", "post" = "gray70")) +
  geom_text(aes(x = 13.5, y = 2.5, label = "Washout period"), color = "#D168CF",size=5) +
  geom_text(aes(x = 36.8, y = -1, label = "Days"), color = "black") +
  #annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 28, xend = 28, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
    annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  geom_rect(aes(xmin = -1, xmax = 1.3, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = -1, y = 0, label = "Code B"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 9, xmax = 11.3, ymin = -1, ymax = 0), color= "gray70", fill="white",alpha = 0.5,linetype=2) +
  geom_text(aes(x = 9, y = 0, label = "Code B"), color = "black", size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 23, xmax = 25.3, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = 23, y = 0, label = "Code C"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 25.8, xmax = 28.0, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = 25.8, y = 0, label = "Code A"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  
  geom_segment(aes(x= 10, y=-2,xend=28, yend=-2), 
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 10, y=-2,xend=0, yend=-2), 
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_text(aes(x = 15, y = -2.1, 
                label = "TAVI case identified with Date of Procedure = Day 0 for Code B"), color = tavi_colour, size=5, vjust=1.6, hjust=0.55) +
  geom_rect(aes(xmin = -10.7, xmax = -8.5, ymin = -1, ymax = 0), 
            color= "gray70", fill="white",alpha = 0.5,linetype=2) +
  geom_text(aes(x = -10.7, y = 0, label = "Code A"), color = "black", size=3, vjust=1.7, hjust=-0.16) +
  geom_segment(aes(x = -10.7, xend = -8.5, y = 0, yend = -1), color = "red") +
  geom_segment(aes(x = -10.7, xend = -8.5, y = -1, yend = 0), color = "red") 


```


```{r, test2, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=2}

df = data.frame(Start_date=c(-10,10),
           End_date=c(10,35),
           variable=c("pre","washout"),
           value=c(1,1)
           )

#0,10,14,31,36

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 5) +
  #scale_y_continuous(limits = c(1,10)) +
  theme(
    plot.margin = margin(10,30,62,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    axis.text.y = element_blank(),   # Remove y-axis labels
    axis.ticks.y = element_blank(),
    plot.title.position = "plot"
    ) +
  labs(title = "Iteration 3") +
  scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(10,17,24,31),labels=c(0,7,14,21)) +
     coord_cartesian(xlim = c(-8.5, 35),ylim = c(1,3),clip = "off") +
  scale_color_manual(values = c("washout" = "#D168CF", "post" = "gray70")) +
  geom_text(aes(x = 22, y = 2.5, label = "Washout period"), color = "#D168CF",size=5) +
  geom_text(aes(x = 36.8, y = -1, label = "Days"), color = "black") +
  

  annotate("segment", x = 10, xend = 10, y = -3, yend = 3, linetype = "dashed", color = "gray70") +

  
  
  geom_rect(aes(xmin = 9, xmax = 11.3, ymin = -1, ymax = 0),fill = "#FFCED7",color= tavi_colour,alpha = 0.5) +
  geom_text(aes(x = 9, y = 0, label = "Code B"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 23, xmax = 25.3, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = 23, y = 0, label = "Code C"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  geom_rect(aes(xmin = 25.8, xmax = 28.0, ymin = -1, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = 25.8, y = 0, label = "Code A"), color = tavi_colour, size=3, vjust=1.7, hjust=-0.16) +
  
  geom_segment(aes(x= 22, y=-2,xend=37, yend=-2), 
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 22, y=-2,xend=10, yend=-2), 
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_text(aes(x = 24.5, y = -2.1, 
                label = "TAVI case discarded as Day 0 is within Iteration 2 washout period"), color = "black", size=5, vjust=1.6, hjust=0.55) +
  geom_rect(aes(xmin = -10.7, xmax = -8.5, ymin = -1, ymax = 0), 
            color= "gray70", fill="white",alpha = 0.5,linetype=2) +
  geom_text(aes(x = -10.7, y = 0, label = "Code A"), color = "black", size=3, vjust=1.7, hjust=-0.16) +
  geom_segment(aes(x = -10.7, xend = -8.5, y = 0, yend = -1), color = "red") +
  geom_segment(aes(x = -10.7, xend = -8.5, y = -1, yend = 0), color = "red") +
  
  
  geom_rect(aes(xmin = -1, xmax = 1.3, ymin = -1, ymax = 0), 
            color= "gray70", fill="white",alpha = 0.5,linetype=2) +
  geom_text(aes(x = -1, y = 0, label = "Code B"), color = "black", size=3, vjust=1.7, hjust=-0.16) +
  geom_segment(aes(x = -1, xend = 1.3, y = 0, yend = -1), color = "red") +
  geom_segment(aes(x = -1, xend = 1.3, y = -1, yend = 0), color = "red")


```


**Other assumptions:**

* An iteration is defined for each date and code, e.g. imagine we are still defining TAVI 1 combinations, if K26 and Y53 were both recorded as at 2022-01-01 then these would be two iterations. The first iteration would check if Y53 and Y49 were found within 28 days of K26 and the second iteration would check is K26 and Y49 were found within 28 days of Y53.
* It may be the case that different TAVI combinations are found within the same washout period. In these cases, they are not treated as independent procedures. E.g. Imagine a TAVI 1 combination was found with date of procedure 2021-01-01 and then a TAVI 3 procedure was found with date of procedure 2021-01-16. Only TAVI 1 is kept and like above TAVI 3 would be discarded as it has been found within the washout period of the last TAVI procedure found.
* Finally, the type of TAVI procedure has been retained in the dataset (the TAVI combination set). In cases where more than one TAVI combination has been identified as at the same date of procedure, both have been listed as the TAVI combination. 


**SAVR case definition:**

We now define SAVR cases. No code combinations are required to define a case however it should be noted that the code that defines a SAVR case, K26, is part of every TAVI case combination. Thus to account for this, **every SAVR case found within a previously defined TAVI washout period is removed**.

Then begins the iterative process to remove cases found in the washout period. This is a simplied version of the washout iterative process defined in TAVI cases. Each date at which a SAVR code is defined becomes an iteration. For each iteration we remove all subsequent codes that follow that lie within the 28 day washout period. We continue this process until no iterations are left and we have identified all SAVR cases, independent of TAVI cases. SAVR case detection is diagrammed in Step 1 and Step 2 below.


**Combining the cohort:**

Finally, we reconcile the TAVI and SAVR cohort to produce our main cohort of aortic valve interventions (SAVR/TAVI).

Note that there will be no cases in which a SAVR procedure is found within a TAVI washout but there may be cases in which a TAVI case is found within a SAVR washout period. Given there is only 1 case in which this has happened, for consistency, the logic to deal with this remains the same as above. That is, the first case (SAVR) is carried forward and the TAVI case discarded. There would be a case to argue that the logic should be updated for cases as such and that the TAVI case should take priority. This can be implemented if appropriate.

The SAVR and TAVI washout reconciliation is diagrammed in Step 3 producing the final result.

<br>


```{r, next, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=2}


df = data.frame(Start_date=c(0,10,38,45,73,85),
           End_date=c(10,38,45,73,85,113),
           variable=c("a","b","a","b","a","a"),
           value=c(1,1)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 5) +
  #scale_y_continuous(limits = c(1,10)) +
  theme(
    plot.margin = margin(10,30,62,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot"
    ) +
  labs(title = "Step 1. Remove SAVR intersections") +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(0, 10, 38, 45, 73, 85)) +
     coord_cartesian(xlim = c(1, 115),ylim = c(1,3),clip = "off") +
  scale_color_manual(values = c("a" = "white", "b" = tavi_colour)) +
  geom_text(aes(x = 24, y = 2.5, label = "TAVI case"), color = tavi_colour,size=5) +
  geom_text(aes(x = 59, y = 2.5, label = "TAVI case"), color = tavi_colour,size=5) +
  geom_text(aes(x = 119.5, y = -1, label = "Days"), color = "black") +
  #annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 10, xend = 10, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 38, xend = 38, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 45, xend = 45, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 73, xend = 73, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  
  #0
  geom_rect(aes(xmin = -1.5, xmax = 1.5, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = -1.5, y = 0, label = "K26"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.16) +
  #7
  geom_rect(aes(xmin = 5.5, xmax = 8.5, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = 5.5, y = 0, label = "K26"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.16) +
  #13
  geom_rect(aes(xmin = 11.5, xmax = 14.5, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = 11.5, y = 0, label = "K26"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.16) +
  geom_segment(aes(x = 11.5, xend = 14.5, y = 0.2, yend = -1), color = "red") +
  geom_segment(aes(x = 11.5, xend = 14.5, y = -1, yend = 0.2), color = "red") +
  #45
  geom_rect(aes(xmin = 43.5, xmax = 46.5, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = 43.5, y = 0, label = "K26"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.16) +
  geom_segment(aes(x = 43.5, xend = 46.5, y = 0.2, yend = -1), color = "red") +
  geom_segment(aes(x = 43.5, xend = 46.5, y = -1, yend = 0.2), color = "red") +
  #85
  geom_rect(aes(xmin = 83.5, xmax = 86.5, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = 83.5, y = 0, label = "K26"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.16)




```

<br>

```{r, next1, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=2}


df = data.frame(Start_date=c(0,28,85,113),
           End_date=c(28,85,113,115),
           variable=c("a","b","a","b"),
           value=c(1,1,1,1)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 5) +
  #scale_y_continuous(limits = c(1,10)) +
  theme(
    plot.margin = margin(10,30,62,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot"
    ) +
  labs(title = "Step 2. Apply SAVR washout iteratively") +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(0, 28, 85, 113)) +
     coord_cartesian(xlim = c(1, 115),ylim = c(1,3),clip = "off") +
  scale_color_manual(values = c("a" = savr_colour, "b" = "white")) +
  geom_text(aes(x = 14, y = 2.5, label = "SAVR Washout period"), color = savr_colour,size=5) +
  geom_text(aes(x = 99, y = 2.5, label = "SAVR Washout period"), color = savr_colour,size=5) +
  geom_text(aes(x = 119.5, y = -1, label = "Days"), color = "black") +
  #annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 28, xend = 28, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 85, xend = 85, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 113, xend = 113, y = -3, yend = 3, linetype = "dashed", color = "gray70") +

  
  #0
  geom_rect(aes(xmin = -1.5, xmax = 1.5, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = -1.5, y = 0, label = "K26"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.16) +
  #7
  geom_rect(aes(xmin = 5.5, xmax = 8.5, ymin = -0.8, ymax = 0), color= "gray70", fill="white",alpha = 0.5,linetype=2) +
  geom_text(aes(x = 5.5, y = 0, label = "K26"), color = "black", size=3, vjust=1.5, hjust=-0.16) +
  geom_segment(aes(x = 5.5, xend = 8.5, y = 0, yend = -0.8), color = "red") +
  geom_segment(aes(x = 5.5, xend = 8.5, y = -0.8, yend = 0), color = "red") +
  #85
  geom_rect(aes(xmin = 83.5, xmax = 86.5, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = 83.5, y = 0, label = "K26"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.16)






```

<br>

```{r, next2, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=2}


df = data.frame(Start_date=c(0,10,28,38,45,73,85,113),
           End_date=c(10,28,38,45,73,85,113,115),
           variable=c("savr","both","tavi","nothing","tavi","nothing","savr","nothing"),
           value=c(1,1,1,1)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 5) +
  #scale_y_continuous(limits = c(1,10)) +
  theme(
    plot.margin = margin(10,30,62,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot"
    ) +
  labs(title = "Step 3. Reconcile SAVR and TAVI cases by applying washout iteratively") +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(0, 10, 28, 38, 45, 73, 85, 113)) +
     coord_cartesian(xlim = c(1, 115),ylim = c(1,3),clip = "off") +
  scale_color_manual(values = c("savr" = savr_colour, "tavi" = tavi_colour, "nothing"="white", "both"="#DA728E")) +
  #geom_text(aes(x = 14, y = 2.5, label = "Washout period"), color = "#D168CF",size=5) +
  #geom_text(aes(x = 99, y = 2.5, label = "Washout period"), color = "#D168CF",size=5) +
  geom_text(aes(x = 119.5, y = -1, label = "Days"), color = "black") +
  #annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 0, xend = 0, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 28, xend = 28, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x =45, xend =45, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 10, xend = 10, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 38, xend = 38, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 73, xend = 73, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 85, xend = 85, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  annotate("segment", x = 113, xend = 113, y = -3, yend = 3, linetype = "dashed", color = "gray70") +
  
  geom_segment(aes(x= 14, y=-1,xend=28, yend=-1), color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 14, y=-1,xend=0, yend=-1), color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) + 
  
  geom_segment(aes(x= 24, y=-2,xend=38, yend=-2), color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 24, y=-2,xend=10, yend=-2), color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 59, y=-1,xend=73, yend=-1), color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 59, y=-1,xend=45, yend=-1), color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 109, y=-1,xend=113, yend=-1), color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 109, y=-1,xend=85, yend=-1),color="#7F7F7F",
               arrow = arrow(angle=30, length=unit(0.1,"npc"), 
                             type = "open")) +
  
geom_text(aes(x = 14, y = -0.8, 
                label = "Iteration 1"), color = "#7F7F7F", size=4.5, vjust=1.6, hjust=0.55) +
geom_text(aes(x = 24, y = -1.8, 
                label = "Iteration 2"), color = "#7F7F7F", size=4.5, vjust=1.6, hjust=0.55) +
geom_text(aes(x = 59, y = -0.8, 
                label = "Iteration 3"), color = "#7F7F7F", size=4.5, vjust=1.6, hjust=0.55) +
geom_text(aes(x = 99, y = -0.8, 
                label = "Iteration 4"), color = "#7F7F7F", size=4.5, vjust=1.6, hjust=0.55)

```

<br>

```{r, next3, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=2}


df = data.frame(Start_date=c(0),
           End_date=c(115),
           variable=c("nothing"),
           value=c(1)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 5) +
  #scale_y_continuous(limits = c(1,10)) +
  theme(
    plot.margin = margin(10,30,62,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot"
    ) +
  labs(title = "Final Result") +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(0, 45, 85)) +
     coord_cartesian(xlim = c(1, 115),ylim = c(1,3),clip = "off") +
  scale_color_manual(values = c("nothing"="white")) +
  geom_text(aes(x = 119.5, y = -1, label = "Days"), color = "black") +
  
  geom_rect(aes(xmin = -2, xmax = 2, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = -2, y = 0, label = "SAVR"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.08) +
  geom_rect(aes(xmin = 83, xmax = 87, ymin = -0.8, ymax = 0), fill = savr_colour, color= "#009FBF", alpha = 0.5) +
  geom_text(aes(x = 83, y = 0, label = "SAVR"), color = "#009FBF", size=3, vjust=1.5, hjust=-0.08) + 
  geom_rect(aes(xmin = 43, xmax = 47, ymin = -0.8, ymax = 0), fill = "#FFCED7", color= tavi_colour, alpha = 0.5) +
  geom_text(aes(x = 43, y = 0, label = "TAVI"), color = tavi_colour, size=3, vjust=1.5, hjust=-0.1)


```




```{r, tidy_cohort, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

#note there are cases where person has same EPIKEY,EPISTART,CODE but different OP DATES for these codes
#ie had more than 1 same type operation under that admission
#thus using OPERATION_DATE instead of EPISTART will make a bigger cohort_full

#clean up procedure codes and derive parent codes
cohort_full = procedure_codes %>% #contains all SAVR abd TAVI possible codes
  mutate(PARENT=substr(CODE,1,3)) %>%
  select(PERSON_ID,OPERATION_DATE,PARENT) %>% #ADMIDATE,EPIKEY,EPISTART 16414496
  distinct() %>%
  arrange(PERSON_ID,OPERATION_DATE,PARENT)

rm(procedure_codes)


```




```{r, tidy_cohort_codes, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# TAVI combinations
tavi1 = c("K26","Y49","Y53")
tavi2 = c("K26","Y49","Y68")
tavi3 = c("K26","Y79","Y53")
tavi4 = c("K26","Y79","Y68")

tavi_combinations = list("tavi1"=tavi1,"tavi2"=tavi2,"tavi3"=tavi3,"tavi4"=tavi4)

washout_period_days = 28


```

```{r, tavi_iterations, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

#runtime 5.7mins

washout_combinations = function(all_combinations, washout_period_days, first_iteration_only = FALSE){
start_time <- Sys.time()
#initialize
iter_persons = ""
result_list = list()

#loop around each set of codes
for(t in 1:length(all_combinations)){
  
final_table = data.frame()
print(paste("Combination: ",names(all_combinations)[t]))

columns_to_filter_all = all_combinations[[t]]
#remove persons who dont have the combination over the full history
all_list = cohort_full %>%
    filter(PARENT%in%all_combinations[[t]]) %>%
    select(PERSON_ID,PARENT) %>%
    distinct() %>%
    pivot_wider(names_from=PARENT,values_from=PARENT,values_fn = list(PARENT = ~1), values_fill = list(PARENT = 0)) %>%
    #for all codes in the combination filter them all to be equal to 1
    filter(across(all_of(columns_to_filter_all), ~ . == 1)) %>%
    select(PERSON_ID) %>%
    pull()

#jump to next iteration if no codes found for anyone across full hx
if (length(all_list)==0) {
    next
  }

#filter the cohort to only include those who have a hx as above and derive iterations to check
cohort = cohort_full %>%
  filter(PERSON_ID%in%all_list) %>%
  filter(PARENT%in%all_combinations[[t]]) %>%
    group_by(PERSON_ID) %>%
    arrange(PERSON_ID,OPERATION_DATE,PARENT) %>%
    mutate(iteration=row_number()) %>% #using row instead of dense_rank(OPERATION_DATE) to capture iterations where they have more than 1 code added on that date
    ungroup()
  

no_of_iterations = cohort %>% summarise(max=max(iteration)) %>% pull()
print(paste("No.of iterations: ", no_of_iterations))

iteration_no = 1
for(i in 1:no_of_iterations){
  
print(iteration_no)
iter = data.frame()

assign(paste0("iter"),
cohort %>%
  #filter(!PERSON_ID %in% (iter_persons) ) %>%
  filter(iteration>(iteration_no-1)) %>%
  group_by(PERSON_ID,PARENT) %>%
  mutate(date_no = row_number()) %>%
  filter(date_no==1) %>%
  mutate(DAY_DATE = if_else(iteration==iteration_no,as.Date(OPERATION_DATE),as.Date(NA))) %>%
  ungroup() %>%
  fill(DAY_DATE, .direction = "down") %>%
  mutate(days_difference = as.numeric(difftime(OPERATION_DATE, DAY_DATE, units = "days"))) %>%
  filter(days_difference<washout_period_days) %>%
  group_by(PERSON_ID) %>%
  mutate(person_count=n()) %>%
  filter(person_count==3) %>%
  select(-date_no,-person_count,-DAY_DATE) %>%
  group_by(PERSON_ID) %>% mutate(iteration = min(iteration, na.rm = TRUE))
)


if(nrow(get(paste0("iter")))>0){
assign(paste0("iter"),
 get(paste0("iter")) %>%
 mutate(iteration = min(iteration, na.rm = TRUE))
) }
  
#iter_persons =  c(iter_persons,(get(paste0("iter")) %>% distinct(PERSON_ID) %>% pull()))


final_table = final_table %>% bind_rows(get(paste0("iter")))
iteration_no = iteration_no + 1


}

result_list[[t]] = final_table %>%
  mutate(combination=names(all_combinations)[t])
  
}

f <- do.call(rbind, result_list)
end_time <- Sys.time()
execution_time <- end_time - start_time
print(execution_time)
return(f)
}


#this returns all the iterations for which a combination is found from that date
tavi_all_iterations = washout_combinations(tavi_combinations, washout_period=28, first_iteration_only = FALSE)
tavi_all_iterations %>% write_rds("tmp/tavi_all_iterations.rds")




```


```{r, tavi_final, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

tavi_all_iterations = read_rds("tmp/tavi_all_iterations.rds")

# For persons who have >1 type of TAVI combination on the same date we have listed them as combination
# Thereafter, the previous surgery iterative method is applied regardless of TAVI type
# i.e. If a TAVI 3 combination is found within a TAVI 1 combination then only TAVI 1 is kept
# in other words the TAVI combinations are not treated as independent procedures

filter_within_washout_period_days = function(dates,washout_period_days) {
  keep_indices = numeric(length(dates))
  last_kept = 1
  
  for (i in 2:length(dates)) {
    if ((dates[i] - dates[last_kept]) > washout_period_days) {
      keep_indices[last_kept] <- 1
      last_kept <- i
    }
  }
  keep_indices[last_kept] = 1
  
  return(keep_indices)
}


tavi_final = tavi_all_iterations %>%
  arrange(PERSON_ID,combination,iteration,OPERATION_DATE,days_difference) %>%
  filter(days_difference==0) %>% #keep all first dates only
  select(-PARENT,-iteration,-days_difference) %>%
  distinct() %>%
  arrange(PERSON_ID,OPERATION_DATE,combination) %>%
  group_by(PERSON_ID, OPERATION_DATE) %>%
  summarize(combination = list(combination)) %>% #for those who have more than one tavi surgery idenfied for that date
  ungroup() %>%
  group_by(PERSON_ID) %>%
  mutate(count_person = n()) %>% 
  #get rid of iterations that fall into the previous iterations
  group_by(PERSON_ID) %>%
  mutate(keep_indices = ifelse(count_person==1,1,
                               filter_within_washout_period_days(OPERATION_DATE, 
                                                                 washout_period_days=washout_period_days))) %>%
  filter(keep_indices == 1) %>%
  ungroup() %>%
  select(-keep_indices,-count_person) %>%
  arrange(PERSON_ID, OPERATION_DATE) %>%
  group_by(PERSON_ID) %>%
  mutate(savr_or_tavi_procedure_number = row_number()) %>%
  mutate(surgery_type="tavi")

tavi_final %>% write_rds("tmp/tavi_final.rds")

```



```{r, savr_final, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

tavi_final = read_rds("tmp/tavi_final.rds")

tavis_to_remove_from_savr = tavi_final %>%
  select(PERSON_ID,OPERATION_DATE) %>%
  mutate(END_WASHOUT = OPERATION_DATE + days(washout_period_days-1)) %>%
  rowwise() %>%
  mutate(DATE = list(seq(OPERATION_DATE, END_WASHOUT, by = "days"))) %>%
  ungroup() %>%
  select(-OPERATION_DATE, -END_WASHOUT) %>%
  unnest(DATE) %>%
  rename(OPERATION_DATE=DATE)

savr_final = cohort_full %>%
  filter(PARENT=="K26") %>%
  distinct() %>%
  mutate(combination="savr") %>%
  #REMOVE ANYONE WHO HAS A TAVI PROCEDURE AT THE SAME DATE (within the washout) AS SAVR (TAVI OVERRIDES THIS)
  anti_join(tavis_to_remove_from_savr,by=c("PERSON_ID","OPERATION_DATE")) %>%
  arrange(PERSON_ID,OPERATION_DATE) %>%
  group_by(PERSON_ID, OPERATION_DATE) %>%
  summarize(combination = list(combination)) %>% #for those who have more than one tavi surgery identified for that date
  ungroup() %>%
  group_by(PERSON_ID) %>%
  mutate(count_person = n()) %>%
  #get rid of iterations that fall into the previous iterations
  group_by(PERSON_ID) %>%
  mutate(keep_indices = ifelse(count_person==1,1,
                               filter_within_washout_period_days(OPERATION_DATE, 
                                                                 washout_period_days=washout_period_days))) %>%
  filter(keep_indices == 1) %>%
  ungroup() %>%
  select(-keep_indices,-count_person) %>%
  arrange(PERSON_ID, OPERATION_DATE) %>%
  group_by(PERSON_ID) %>%
  mutate(savr_or_tavi_procedure_number = row_number()) %>%
  mutate(surgery_type="savr")


savr_final %>% write_rds("tmp/savr_final.rds")

```

```{r, overall_final, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

savr_final = read_rds("tmp/savr_final.rds")

# NEED TO APPLY THE WASHOUT AGAIN

overall_final_no_washout = savr_final %>%
  bind_rows(tavi_final) %>%
  arrange(PERSON_ID,OPERATION_DATE) %>%
  ungroup() %>%
  group_by(PERSON_ID) %>%
  mutate(procedure_number = row_number()) %>%
  mutate(overall_no_procedures = n()) %>%
  ungroup()


# find people who have a TAVI procedure within an earlier SAVR procedure
people_who_have_tavi_in_savr_washout = overall_final_no_washout %>% filter(overall_no_procedures>1) %>%
  arrange(PERSON_ID, OPERATION_DATE) %>%
  group_by(PERSON_ID) %>%
  mutate(previous_surgery = lag(surgery_type)) %>%
  ungroup() %>%
  filter(surgery_type == 'tavi' & previous_surgery == 'savr') %>%
  distinct(PERSON_ID) %>%
  left_join(overall_final_no_washout) %>%
  select(-savr_or_tavi_procedure_number) %>%
  arrange(PERSON_ID,OPERATION_DATE)  %>%
  group_by(PERSON_ID) %>%
  mutate(date_diff = as.numeric(OPERATION_DATE - lag(OPERATION_DATE, default = first(OPERATION_DATE)))) %>%
  ungroup() %>%
  filter(date_diff>0 & date_diff<28) %>%
  distinct(PERSON_ID) %>% left_join(overall_final_no_washout)

#there is only one person - as per the methodology the SAVR will be carried forward
people_who_have_tavi_in_savr_washout %>% distinct(PERSON_ID) %>% count() %>% pull()


overall_final = 
people_who_have_tavi_in_savr_washout %>% distinct(PERSON_ID) %>% mutate(savr_tavi_remove=1) %>%
  right_join(overall_final_no_washout) %>%
  mutate(savr_tavi_remove = ifelse(is.na(savr_tavi_remove),0,savr_tavi_remove)) %>%
  #get rid of iterations that fall into the previous iterations
  group_by(PERSON_ID) %>%
  mutate(keep_indices = ifelse(savr_tavi_remove==0,1,
                               filter_within_washout_period_days(OPERATION_DATE, 
                                                                 washout_period_days=washout_period_days))) %>%

  filter(keep_indices == 1) %>%
  ungroup() %>%
  select(-keep_indices) %>%
  arrange(PERSON_ID, OPERATION_DATE) %>%
  select(-savr_or_tavi_procedure_number,-savr_tavi_remove)



# now select FIRST procedure only (noting that some TAVIs may have had SAVR first)
# also noting that these are first procedures but not necessarily first procedures for AS
overall_final_first = overall_final %>%
  filter(procedure_number==1)




overall_final %>% write_rds("tmp/overall_final.rds")
overall_final_first %>% write_rds("tmp/overall_final_first.rds")


```



* As seen in Table \@ref(tab:firstSurgeryCount), and as expected, SAVR procedures make up the majority of the first procedure cohort.

* Figure \@ref(fig:surgeryCoveragePlot) shows that there is sufficient coverage from 2000 for SAVR cases. It can be seen that as TAVI procedures increase from ~2009, the number of SAVR procedures declines. As aforementioned AS to procedure lead time will be explored later in order to confirm that a study start date as at 2020-01-01 is sufficient to capture those undergoing SAVR for aortic stenosis specifically.


```{r, overall_read, warning=FALSE, message=FALSE, error=FALSE, eval=TRUE, echo=FALSE}

overall_final = read_rds("tmp/overall_final.rds")
overall_final_first = read_rds("tmp/overall_final_first.rds")

```



```{r, surgery_coverage_working, warning=FALSE, message=FALSE, error=FALSE, eval=FALSE, echo=FALSE}


count_data = overall_final %>%
  select(PERSON_ID,OPERATION_DATE,surgery_type) %>%
  #filter(OPERATION_DATE>"1994-01-01") %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  group_by(PERSON_ID,year_month,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR,surgery_type) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))

count_data_min = count_data%>%ungroup()%>%summarize(min_date = min(year_month_date))%>%pull()
count_data_max = count_data%>%ungroup()%>%summarize(max_date = max(year_month_date))%>%pull()

full_date_df = data.frame(year_month_date = seq(
  count_data_min,
  count_data_max,
  by = "1 month")) %>%
  left_join((count_data%>%ungroup()%>%select(-PERSON_ID)%>%mutate(distinct=1))) %>%
  mutate(count = coalesce(count, 0),
         distinct = coalesce(distinct, 0)) %>%
  mutate(OPERATION_DATE_MONTH = month(year_month_date)) %>%
  mutate(OPERATION_DATE_YEAR = year(year_month_date)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  group_by(year_month,surgery_type) %>%
  summarise(count=sum(count),distinct=sum(distinct)) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  pivot_longer(cols=c(count,distinct),names_to="group") %>%
  mutate(group=ifelse(group=="count","Records with valid IDs", "Distinct IDs")) %>%
  mutate(group = factor(group, levels = c("Records with valid IDs","Distinct IDs"))) %>%
  select(-group) %>% distinct() %>% rename(group=surgery_type) %>% ungroup()

full_date_df %>% write_rds("tmp/surgery_coverage_data.rds")

```


```{r, surgeryCoveragePlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="Aortic Valve Interventions identified, using the case methodology - including repeat procedures, in HES APC"}


full_date_df = read_rds("tmp/surgery_coverage_data.rds")

full_date_df = full_date_df %>% mutate(year=as.numeric(substr(year_month,1,4))) %>%
  filter(year>=1997 & year<2024)


#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))


label_years = seq(min_break_year,max_break_label,2)
break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month,year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 

ggplot((full_date_df%>%mutate(group=ifelse(group=="savr","SAVR","TAVI"))), aes(x = year_month_label, y = value, group = group, color=group)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt")),
    legend.position = c(0.01, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt")
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("TAVI" = tavi_colour, "SAVR" = savr_colour)) +
  geom_vline(xintercept = "2000-01", linetype = "dashed", color = "gray70") +
  geom_text(aes(x = "2000-01", y = 250, label = "Study start date"), hjust = -0.05, color = "gray70") 




```





```{r, SurgeryCount, warning=FALSE, message=FALSE, error=FALSE, eval=TRUE, echo=FALSE}


surgery_table = overall_final %>% count(surgery_type) %>%
  mutate(PCT=round(n/sum(n)*100,1)) %>%
  mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI")) %>%
  rename(`Surgery Type`=surgery_type, N=n)

kableExtra::kbl(
  (surgery_table%>%
     bind_rows(surgery_table %>%
                 summarise(N = sum(N, na.rm = TRUE)) %>%
                 mutate(PCT=100.0) %>%
                 mutate(`Surgery Type`="")) %>%
     #apply SDC
     mutate(N = case_when(
     N < 10 ~ 10,
     TRUE ~ round(N / 5) * 5
     )) %>%
     mutate(N=format(N, big.mark = ","))
     ),
    booktabs = T, 
    caption=("Surgery types defining the procedures cohort <br> SDC applied"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )


```





# Main Cohort

We now take the cohort derived in [Main Procedure Cohort] and apply the main inclusion and exclusion criteria. That is, 

* find those who have a stenosis pathology, using the defined selection groups and the cohort defined in [Aortic Stenosis]
* apply the study start date as at 2000-01-01, i.e. exclude those who have a previous SAVR/TAVI, prior to the study start date as at 2000-01-01
* exclude repeat procedures (first SAVR/TAVI procedures considered only)

Note that the following exclusions have not yet been applied:

* age < 18 years old, as age has not yet been derived
* no study end date has been applied

## First and subsequent procedures


```{r, as_codes_first, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

as_first_codes = as_codes %>%
  filter(EPISTART >= "1995-01-01") %>%
  #filter to only include anyone identified in the procedures cohort
  filter(PERSON_ID %in% (overall_final_first%>%distinct(PERSON_ID)%>%pull())) %>%
  select(PERSON_ID,EPISTART) %>%
  arrange(PERSON_ID,EPISTART) %>%
  group_by(PERSON_ID) %>%
  mutate(as_order = row_number()) %>%
  filter(as_order==1) %>%
  select(-as_order) %>%
  distinct() %>%
  rename(AS_FIRST = EPISTART)


as_first_codes %>% write_rds("tmp/as_first_codes.rds")

```


```{r, setCounts, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

as_first_codes = read_rds("tmp/as_first_codes.rds")

#first procedures only
main_cohort_with_selections = 
  overall_final_first %>%
  #filter for procedures >= 2000-01-01 (this also removes those who had surgery prior to 2000 as this is FIRST surgeries only)
  filter(OPERATION_DATE >= "2000-01-01") %>%
  # AS selection priority groups
  left_join(as_first_codes) %>%
  mutate(as_procedure_lt = as.numeric(OPERATION_DATE - AS_FIRST)) %>%
  mutate(AS_FIRST_SUB_6_MONTHS = AS_FIRST %m-% months(6)) %>% #AS_FIRST - as.period(months(6), start = AS_FIRST)
  mutate(as_selection_group = case_when(
    as_procedure_lt >= 0 ~ 1,
    (as_procedure_lt < 0 & as.numeric(OPERATION_DATE - AS_FIRST_SUB_6_MONTHS) >= 0)~ 2,
    .default = 3
  )) %>%
  mutate(as_selection_group = ifelse(is.na(AS_FIRST),NA,as_selection_group))



sets_table = main_cohort_with_selections %>% count(surgery_type,as_selection_group) %>% 
  pivot_wider(names_from=surgery_type,values_from=n) %>%
  mutate(overall = savr+tavi) %>%
  mutate(pct_savr=round(savr/sum(savr)*100,1),pct_tavi=round(tavi/sum(tavi)*100,1),
         pct_overall=round(overall/sum(overall)*100,1)) %>%
  #apply SDC
     mutate(overall = case_when(
     overall < 10 ~ 10,
     TRUE ~ round(overall / 5) * 5
     )) %>%
     mutate(savr = case_when(
     savr < 10 ~ 10,
     TRUE ~ round(savr / 5) * 5
     )) %>%
     mutate(tavi = case_when(
     tavi < 10 ~ 10,
     TRUE ~ round(tavi / 5) * 5
     )) %>%
  mutate(savr = paste0(format(savr, big.mark = ",")," (",pct_savr,"%)"),
         tavi = paste0(format(tavi, big.mark = ",")," (",pct_tavi,"%)"),
         overall = paste0(format(overall, big.mark = ",")," (",pct_overall,"%)")) %>%
  select(-pct_savr,-pct_tavi,-pct_overall) %>%
  mutate(as_selection_group=case_when(
    as_selection_group ==1 ~ "AS code found in Priority Group 1",
    as_selection_group ==2 ~ "AS code found in Priority Group 2",
    as_selection_group ==3 ~ "AS code found outside Priority Groups",
    is.na(as_selection_group) ~  "No AS code found"
  )) %>%
  rename(`Main Procedure + Aortic Stenosis cohort sets`=as_selection_group,SAVR=savr,TAVI=tavi,Overall=overall)



```


```{r, setsCoveragePlot_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

count_data = main_cohort_with_selections %>%
  filter(as_selection_group%in%c(1,2)) %>% #all first procedures regardless of if they have an AS pathology
  select(PERSON_ID,OPERATION_DATE) %>%
  mutate(group = "as_only") %>%
  bind_rows(main_cohort_with_selections %>%
  select(PERSON_ID,OPERATION_DATE) %>%
  mutate(group = "all") ) %>%
  #filter(OPERATION_DATE>"1994-01-01") %>%
  mutate(OPERATION_DATE_MONTH = month(OPERATION_DATE),
         OPERATION_DATE_YEAR = year(OPERATION_DATE)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  group_by(PERSON_ID,year_month,OPERATION_DATE_MONTH,OPERATION_DATE_YEAR,group) %>%
  summarize(count = n()) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(year_month_date=as.Date(paste0(year_month_label,"-01")))



count_data_min = count_data%>%ungroup()%>%summarize(min_date = min(year_month_date))%>%pull()
count_data_max = count_data%>%ungroup()%>%summarize(max_date = max(year_month_date))%>%pull()

full_date_df = data.frame(year_month_date = seq(
  count_data_min,
  count_data_max,
  by = "1 month")) %>%
  left_join((count_data%>%ungroup()%>%select(-PERSON_ID)%>%mutate(distinct=1))) %>%
  mutate(count = coalesce(count, 0),
         distinct = coalesce(distinct, 0)) %>%
  mutate(OPERATION_DATE_MONTH = month(year_month_date)) %>%
  mutate(OPERATION_DATE_YEAR = year(year_month_date)) %>%
  mutate(year_month = paste(OPERATION_DATE_YEAR, OPERATION_DATE_MONTH, sep = "-")) %>%
  group_by(year_month,group) %>%
  summarise(count=sum(count),distinct=sum(distinct)) %>%
  mutate(year_month_label = ifelse(str_length(year_month)==6,
                                   paste0(substr(year_month, 1, 5),"0",substr(year_month, 6, 6)),
                                   year_month)) %>%
  mutate(group=ifelse(group=="all","All Aortic Valve Replacements", "Aortic Valve Replacements with AS pathology")) %>%
  distinct() %>%
  ungroup()

full_date_df %>% write_rds("tmp/surgery_as_coverage_data.rds")


```



```{r, setsCoveragePlot, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE, fig.width=12, fig.height=7, fig.cap="Aortic Valve Replacement (SAVR and TAVI combined) Coverage - first procedures only"}


full_date_df = read_rds("tmp/surgery_as_coverage_data.rds")

#plot labels
min_break_year = if(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),6,7)=="01"){
  as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))
  } else {
    as.numeric(substr(full_date_df%>%summarize(min_date = min(year_month_label))%>%pull(),1,4))+1
    }

max_break_label = as.numeric(substr(full_date_df%>%summarize(max_date = max(year_month_label))%>%pull(),1,4))


label_years = seq(min_break_year,max_break_label,2)

break_years = full_date_df %>%
  mutate(year=as.numeric(substr(year_month,1,4))) %>%
  select(year,year_month, year_month_label) %>%
  right_join(data.frame(year=label_years)) %>%
  filter(str_ends(year_month,"-1")) %>%
  distinct() %>%
  pull(year_month_label) 

max_date_as_at = full_date_df %>% distinct(year_month_label) %>% arrange(year_month_label) %>% tail(1) %>% pull()

ggplot((full_date_df), aes(x = year_month_label, y = count, group = group, color=group)) +
  geom_line(size = 1) +
  labs(x = "Date", y = "Number per month"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +
  scale_x_discrete(breaks=break_years,labels=label_years) +
  scale_y_continuous(labels = scales::comma, limits=c(0,1800), breaks=c(0,500,1000,1500)) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt")),
    legend.position = c(0.01, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt")
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("All Aortic Valve Replacements"="#FFD2B3", "Aortic Valve Replacements with AS pathology"="#FF6800")) +
  
  geom_vline(xintercept = "2005-01", linetype = "dashed", color = "gray70") +
  geom_vline(xintercept = "2015-01", linetype = "dashed", color = "gray70") +
  geom_vline(xintercept = "2020-01", linetype = "dashed", color = "gray70") +
  
  geom_segment(aes(x= "2000-01", xend="2005-01", y=1350, yend=1350), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= "2005-01", xend="2000-01", y=1350, yend=1350), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= "2015-01", xend="2020-01", y=1350, yend=1350), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= "2020-01", xend="2015-01", y=1350, yend=1350), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= "2000-01", xend="2020-01", y=1500, yend=1500), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= "2020-01", xend="2000-01", y=1500, yend=1500), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= "2020-01", xend=max_date_as_at, y=1350, yend=1350), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= max_date_as_at, xend="2020-01", y=1350, yend=1350), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= "2020-01", xend=max_date_as_at, y=1500, yend=1500), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= max_date_as_at, xend="2020-01", y=1500, yend=1500), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  
  geom_text(aes(x = "2008-07", y = 1600, label = "Prior to Pandemic"), hjust = -0.05, color = "#475ca6") +
  geom_text(aes(x = "2020-10", y = 1600, label = "Pandemic"), hjust = -0.05, color = "#475ca6") +
  
  geom_text(aes(x = "2001-09", y = 1400, label = "Beginning"), hjust = -0.05, color = "#475ca6") +
  geom_text(aes(x = "2017-03", y = 1400, label = "End"), hjust = -0.05, color = "#475ca6") +
  geom_text(aes(x = "2021-01", y = 1400, label = "During"), hjust = -0.05, color = "#475ca6")




```


```{r, main_cohort_working, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

#for each persons surgeries - add a AS start date and end date for which AS would fall into a selection group
#for that particular surgery
surgery_as_selection_groups = overall_final %>%
  select(PERSON_ID,OPERATION_DATE=OPERATION_DATE,procedure_number,overall_no_procedures) %>%
  arrange(PERSON_ID,OPERATION_DATE) %>%
  mutate(INFINITY=as.Date("1900-01-01")) %>%
  mutate(OPERATION_DATE_plus_6months=OPERATION_DATE %m+% months(6)) %>%
  group_by(PERSON_ID) %>%
  mutate(previous_surgery_end_date = lag(OPERATION_DATE_plus_6months)) %>%
  ungroup() %>%
  mutate(AS_GROUP_START_DATE = if_else(procedure_number==1,INFINITY,previous_surgery_end_date),
         AS_GROUP_END_DATE = OPERATION_DATE_plus_6months) %>%
  select(-INFINITY,-OPERATION_DATE_plus_6months,-previous_surgery_end_date)

# now for each persons AS codes add on these selection groups and see which ones have surgery selection groups
procedure_earliest_as_codes = as_codes %>%
  filter(PERSON_ID %in% (surgery_as_selection_groups%>%select(PERSON_ID)%>%pull)) %>%
  select(PERSON_ID,EPISTART) %>%
  distinct() %>%
  arrange(PERSON_ID,EPISTART) %>%
  rename(AS_DATE=EPISTART) %>%
  filter(AS_DATE >= "1995-01-01") %>%
  left_join(surgery_as_selection_groups,by="PERSON_ID") %>%
  arrange(PERSON_ID,AS_DATE) %>%
  arrange(PERSON_ID,OPERATION_DATE,AS_DATE) %>%
  #this will contain all AS codes that fall into op selection period. Filter for earliest
  filter(AS_DATE > AS_GROUP_START_DATE & AS_DATE <= AS_GROUP_END_DATE) %>%
  relocate(OPERATION_DATE, .after=AS_DATE) %>%
  group_by(PERSON_ID,OPERATION_DATE) %>% mutate(as_no=row_number()) %>% ungroup() %>%
  #this will now exclude the procedures for which someone doesnt have an AS code
  #so persons prev thought to have multiple procedures may only have one now
  filter(as_no==1) %>% select(-as_no) %>%
  select(PERSON_ID,OPERATION_DATE,AS_EARLIST_DATE=AS_DATE)


surgery_with_as_earlist = 
  surgery_as_selection_groups %>%
  left_join(procedure_earliest_as_codes, by=c("PERSON_ID","OPERATION_DATE"))

##########################################################################################
# CHECK

new = surgery_with_as_earlist %>%
  filter(!is.na(AS_EARLIST_DATE)) %>%
  filter(procedure_number==1) %>%
  filter(OPERATION_DATE >= "2000-01-01") %>%
  select(PERSON_ID,OPERATION_DATE)

old = 
main_cohort_with_selections %>% filter(procedure_number==1) %>% filter(!is.na(AS_FIRST)) %>%
  filter(as_selection_group%in%c(1,2)) %>% select(PERSON_ID,OPERATION_DATE =OPERATION_DATE)


#whats in old that isnt in new
check = 
setdiff(old,new) %>% select(PERSON_ID) %>% pull(PERSON_ID)

#whats been picked up in new that wasnt in old
setdiff(new,old)

##########################################################################################



# now apply the start date condition and get rid of those who dont have AS code for their surgery

people_with_first_surgery_post_study_start = surgery_with_as_earlist %>%
  filter(!is.na(AS_EARLIST_DATE)) %>%
  filter(procedure_number==1) %>%
  filter(OPERATION_DATE >= "2000-01-01") %>%
  select(PERSON_ID)
  

# Main Cohort Final - has the start date exlusion and now only contains procedures for AS
main_cohort_final = people_with_first_surgery_post_study_start %>%
  left_join(surgery_with_as_earlist) %>%
  filter(!is.na(AS_EARLIST_DATE)) %>%
  #recompute procedure numbers as now only have procedures for AS
  select(-procedure_number) %>%
  ungroup() %>%
  arrange(PERSON_ID,OPERATION_DATE) %>%
  group_by(PERSON_ID) %>%
  mutate(procedure_as_number = row_number()) %>%
  mutate(overall_no_as_procedures = n()) %>%
  ungroup() %>%
  select(PERSON_ID,OPERATION_DATE,AS_EARLIST_DATE,procedure_as_number,overall_no_as_procedures,overall_no_procedures,
         AS_GROUP_START_DATE,AS_GROUP_END_DATE) %>%
  left_join((overall_final%>%select(PERSON_ID,OPERATION_DATE=OPERATION_DATE,combination,surgery_type)),
            by=c("PERSON_ID","OPERATION_DATE"))




main_cohort_final_first = main_cohort_final %>%
  filter(procedure_as_number==1)


main_cohort_final %>% write_rds("tmp/main_cohort_final.rds")
main_cohort_final_first %>% write_rds("tmp/main_cohort_final_first.rds")

```


It may be the case that whilst a persons first procedure had a pathology different to AS, a subsequent/repeat procedure took place with an AS pathology. In this case the persons first procedure is redefined to mean the persons first procedure for AS.

We assume that where an AS code cannot be found in the selection group related to a procedure, that the procedure was performed for another reason (e.g. Regurgitation).

In other words, we further restrict the procedure cohort to only include procedures that were performed for AS. We now consider AS codes in relation to a persons procedure timeline, instead of considering only their first AS code found.


This is diagrammed on a timeline below:


```{r, diag1, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=3}


df = data.frame(Start_date=c(0,50.8,0,50.8,0,50.8),
           End_date=c(50.4,100,50.4,100,50.4,100),
           variable=c("a","a","a","a","a","a"),
           value=c(2,2,7,7,12,12)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 10) +
  scale_y_continuous(limits = c(0,11)) +
  theme(
    plot.margin = margin(10,55,10,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    #axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot",
    plot.title = element_text(margin=margin(0,0,20,0))
    ) +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(17), labels=c("Study Start Date")) +
  scale_y_continuous(breaks = c(2,7,12), labels=c("Person 3","Person 2","Person 1"), limits=c(-1,20)) +
  coord_cartesian(xlim = c(0,110), clip = "off",ylim = c(-2,20),) +
  scale_color_manual(values = c("a" = "#FFD2B3", "b" = "white")) +
  
  geom_vline(xintercept = 50.6, linetype = "dashed", color = "gray70") +
  
  geom_label(aes(x = 35, y = 2, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 35, y = 7, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 35, y = 12, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  
  #geom_text(aes(x = 10, y = 1,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  geom_text(aes(x = 10, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  geom_text(aes(x = 10, y = 12,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  
  geom_text(aes(x = 65, y = 2,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  geom_text(aes(x = 65, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  #geom_text(aes(x = 65, y = 5,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  
  geom_label(aes(x = 85, y = 2, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 85, y = 7, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 85, y = 12, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  labs(title = "Events found in the main procedure cohort and AS cohort in chronological order") +
  
  geom_text(aes(x = 108, y = 16.4,label = "\u2605"), size = 9, vjust=0.25, color = "red") +
  geom_text(aes(x = 115, y = 15,label = "First AS code\nfound in\nselection group"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_segment(aes(x= 0, xend=50.4, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=0, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 50.8, xend=100, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=50.8, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 42, xend=50.4, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=42, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 46, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 95.7, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_segment(aes(x= 91.5, xend=100, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=91.5, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 25, y = 20,label = "Procedure 1 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  geom_text(aes(x = 75, y = 20,label = "Procedure 2 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 113.5, y = -1, label = "Days"), color = "black", vjust=3.6)
  
  
  

```


```{r, diag2, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=3}


df = data.frame(Start_date=c(0,50.8,0,50.8,0,50.8),
           End_date=c(50.4,100,50.4,100,50.4,100),
           variable=c("a","a","a","a","a","a"),
           value=c(2,2,7,7,12,12)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 10) +
  scale_y_continuous(limits = c(0,11)) +
  theme(
    plot.margin = margin(10,55,10,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    #axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot",
    plot.title = element_text(margin=margin(0,0,20,0))
    ) +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(17), labels=c("Study Start Date")) +
  scale_y_continuous(breaks = c(2,7,12), labels=c("Person 3","Person 2","Person 1"), limits=c(-1,20)) +
  coord_cartesian(xlim = c(0,110), clip = "off",ylim = c(-2,20),) +
  scale_color_manual(values = c("a" = "#FFD2B3", "b" = "white")) +
  
  geom_vline(xintercept = 50.6, linetype = "dashed", color = "gray70") +
  
  #geom_label(aes(x = 35, y = 2, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 35, y = 7, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 35, y = 12, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  
  #geom_text(aes(x = 10, y = 1,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  geom_text(aes(x = 10, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  geom_text(aes(x = 10, y = 12,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  
  geom_text(aes(x = 65, y = 2,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  geom_text(aes(x = 65, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  #geom_text(aes(x = 65, y = 5,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  
  geom_label(aes(x = 85, y = 2, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 85, y = 7, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  #geom_label(aes(x = 85, y = 12, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  labs(title = "Events remaining after AS and procedure inclusion/exclusion applied") +

  
  geom_segment(aes(x= 0, xend=50.4, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=0, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 50.8, xend=100, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=50.8, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 42, xend=50.4, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=42, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 46, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 95.7, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_segment(aes(x= 91.5, xend=100, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=91.5, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 25, y = 20,label = "Procedure 1 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  geom_text(aes(x = 75, y = 20,label = "Procedure 2 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 113.5, y = -1, label = "Days"), color = "black", vjust=3.6)
  
  
  

```


* Noting that we only include those who had their first SAVR/TAVI procedure for AS after the study start date. If the study start date above was shifted to after Procedure 1 the cohort that remains would be as follows:


```{r, diag3, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=3}


df = data.frame(Start_date=c(0,50.8,0,50.8,0,50.8),
           End_date=c(50.4,100,50.4,100,50.4,100),
           variable=c("a","a","a","a","a","a"),
           value=c(2,2,7,7,12,12)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 10) +
  scale_y_continuous(limits = c(0,11)) +
  theme(
    plot.margin = margin(10,55,10,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    #axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot",
    plot.title = element_text(margin=margin(0,0,20,0))
    ) +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(45), labels=c("Study Start Date")) +
  scale_y_continuous(breaks = c(2,7,12), labels=c("Person 3","Person 2","Person 1"), limits=c(-1,20)) +
  coord_cartesian(xlim = c(0,110), clip = "off",ylim = c(-2,20),) +
  scale_color_manual(values = c("a" = "#FFD2B3", "b" = "white")) +
  
  geom_vline(xintercept = 50.6, linetype = "dashed", color = "gray70") +
  
  geom_label(aes(x = 35, y = 2, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 35, y = 7, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 35, y = 12, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  
  #geom_text(aes(x = 10, y = 1,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  geom_text(aes(x = 10, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  geom_text(aes(x = 10, y = 12,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  
  geom_text(aes(x = 65, y = 2,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  geom_text(aes(x = 65, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  #geom_text(aes(x = 65, y = 5,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  
  geom_label(aes(x = 85, y = 2, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 85, y = 7, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  geom_label(aes(x = 85, y = 12, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  labs(title = "Events found in the main procedure cohort and AS cohort in chronological order") +
  
  geom_text(aes(x = 108, y = 16.4,label = "\u2605"), size = 9, vjust=0.25, color = "red") +
  geom_text(aes(x = 115, y = 15,label = "First AS code\nfound in\nselection group"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_segment(aes(x= 0, xend=50.4, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=0, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 50.8, xend=100, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=50.8, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 42, xend=50.4, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=42, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 46, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 95.7, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_segment(aes(x= 91.5, xend=100, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=91.5, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 25, y = 20,label = "Procedure 1 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  geom_text(aes(x = 75, y = 20,label = "Procedure 2 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 113.5, y = -1, label = "Days"), color = "black", vjust=3.6)
  
  
  

```

```{r, diag4, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12, fig.height=3}


df = data.frame(Start_date=c(0,50.8,0,50.8,0,50.8),
           End_date=c(50.4,100,50.4,100,50.4,100),
           variable=c("a","a","a","a","a","a"),
           value=c(2,2,7,7,12,12)
           )

# Plot results
ggplot(df) + geom_segment(aes(x = Start_date, 
                              xend = End_date, 
                              y = value, 
                              yend = value, 
                              colour = variable), size = 10) +
  scale_y_continuous(limits = c(0,11)) +
  theme(
    plot.margin = margin(10,55,10,30),
    axis.line.y = element_blank(),
    axis.line.x = element_line(color = "black",arrow = arrow(type = "closed")),
    panel.grid.major = element_blank(),  # Remove major gridlines
    panel.grid.minor = element_blank(),  # Remove minor gridlines
    panel.background = element_rect(fill = "white"),
    legend.position = "none",
    axis.title = element_blank(),  # Remove y-axis title
    #axis.text.y = element_blank(),  
    axis.ticks.y = element_blank(),
    plot.title.position = "plot",
    plot.title = element_text(margin=margin(0,0,20,0))
    ) +
  #scale_y_continuous(limits = c(-10,3)) +
  scale_x_continuous(breaks = c(45), labels=c("Study Start Date")) +
  scale_y_continuous(breaks = c(2,7,12), labels=c("Person 3","Person 2","Person 1"), limits=c(-1,20)) +
  coord_cartesian(xlim = c(0,110), clip = "off",ylim = c(-2,20),) +
  scale_color_manual(values = c("a" = "#FFD2B3", "b" = "white")) +
  
  geom_vline(xintercept = 50.6, linetype = "dashed", color = "gray70") +
  
  #geom_label(aes(x = 35, y = 2, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  #geom_label(aes(x = 35, y = 7, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  #geom_label(aes(x = 35, y = 12, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  
  #geom_text(aes(x = 10, y = 1,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  #geom_text(aes(x = 10, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  #geom_text(aes(x = 10, y = 12,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  
  geom_text(aes(x = 65, y = 2,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  #geom_text(aes(x = 65, y = 7,label = "\u2605"), size = 9, vjust=0.21, color = "red") +
  #geom_text(aes(x = 65, y = 5,label = "\u2605"), size = 10, vjust=0.22, color = "red") +
  
  geom_label(aes(x = 85, y = 2, label = "Procedure 1"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  #geom_label(aes(x = 85, y = 7, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  #geom_label(aes(x = 85, y = 12, label = "Procedure 2"), color = "#D168CF", fontface ="bold", fill = "#FFD2B3", size=4) +
  labs(title = "Events remaining after AS and procedure inclusion/exclusion applied") +

  
  geom_segment(aes(x= 0, xend=50.4, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=0, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 50.8, xend=100, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=50.8, y=18.6, yend=18.6), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.07,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 42, xend=50.4, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 50.4, xend=42, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 46, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 95.7, y = 15,label = "6 months"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_segment(aes(x= 91.5, xend=100, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 100, xend=91.5, y=14, yend=14), color = "#475ca6", size = 0.3,
               arrow = arrow(angle=30, length=unit(0.05,"npc"), 
                             type = "open")) +
  
  geom_text(aes(x = 25, y = 20,label = "Procedure 1 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  geom_text(aes(x = 75, y = 20,label = "Procedure 2 AS selection period"), vjust=0.25, color = "black",
            fontface = "plain", size=3) +
  
  geom_text(aes(x = 113.5, y = -1, label = "Days"), color = "black", vjust=3.6)
  
  
  

```



It should be noted that in the majority of cases a persons first SAVR/TAVI for AS is found after the study start date.



```{r, mainCohortCount, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


main_cohort_final = read_rds("tmp/main_cohort_final.rds")
main_cohort_final_first = read_rds("tmp/main_cohort_final_first.rds")


count_table = 
overall_final %>% count(surgery_type) %>% pivot_wider(values_from=n,names_from=surgery_type) %>%
  mutate(overall=savr+tavi) %>% mutate(name="Total number of procedures") %>%
  
bind_rows((

main_cohort_final %>% count(surgery_type) %>% pivot_wider(values_from=n,names_from=surgery_type) %>%
  mutate(overall=savr+tavi) %>% mutate(name="Total number of procedures with as AS pathology")

)) %>% bind_rows((

main_cohort_final_first %>% count(surgery_type) %>% pivot_wider(values_from=n,names_from=surgery_type) %>%
  mutate(overall=savr+tavi) %>% mutate(name="Procedures with as AS pathology - first procedures only")

)) %>%
  
#apply SDC
     mutate(overall = case_when(
     overall < 10 ~ 10,
     TRUE ~ round(overall / 5) * 5
     )) %>%

     mutate(savr = case_when(
     savr < 10 ~ 10,
     TRUE ~ round(savr / 5) * 5
     )) %>%
  
     mutate(tavi = case_when(
     tavi < 10 ~ 10,
     TRUE ~ round(tavi / 5) * 5
     )) %>%
  
select(" "=name,SAVR=savr,TAVI=tavi,Overall=overall)

 kableExtra::kbl(
(count_table %>%
     mutate(SAVR=format(SAVR, big.mark = ",")) %>%
      mutate(TAVI=format(TAVI, big.mark = ",")) %>%
      mutate(Overall=format(Overall, big.mark = ","))
      ),
     booktabs = T, 
     caption=("Main Cohort after applying study start date and AS (broad definition) inclusions/exclusions<br> SDC applied"),
     linesep = "",
     align="l"
     ) %>%
   kableExtra::column_spec(4, bold = TRUE)  %>% #bold last column
   kableExtra::kable_styling(
     #font_size
     )


```




```{r, save, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

#WRITE COHORT BACK TO DB - such that skinny and LSOA can be applied to these patientIDs - note that we NEED the procedure dates and patientIDs for LSOA as we are taking this as close to procedure as possible. For the skinny we actually dont need this data.

data_to_write = main_cohort_final_first %>% select(PERSON_ID,OPERATION_DATE) %>%
  mutate(PERSON_ID=as.character(PERSON_ID),
         OPERATION_DATE=as.character(OPERATION_DATE))


#not working
DBI::dbWriteTable(con, 
                  Id(schema = "dsa_391419_j3w9t_collab", table = "ccu056_main_cohort_final_first"),
                  value=data_to_write, 
                  overwrite=TRUE, append=FALSE,
                  row.names = FALSE,
                  field.types = c(
                    PERSON_ID = "varchar(255)",
                    OPERATION_DATE = "varchar(255)"
                    )
                  )



main_cohort_final_first %>% select(PERSON_ID,OPERATION_DATE) %>% write_csv("tmp/main_cohort_final_first_persons.csv")

# COPIED TO DATABRICKS NOTEBOOK - MAIN COHORT

```


## What codes are driving SAVR cases?

Note that repair codes K265, K268, K269 are included.

```{r, savr_cases, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

main_first_simple = main_cohort_final_first %>%
  select(PERSON_ID, OPERATION_DATE, surgery_type, combination)

procedure_codes <- dbGetQuery(con,sprintf(glue(select_all_proj,"tmp_cases_procedure_codes")))

procedure_codes_op_dates <- dbGetQuery(con,sprintf(glue(select_all_proj,
                                                        "tmp_cases_procedure_codes_operation_dates")))

# same amount of rows as procedure codes
procedure_codes_joined = procedure_codes %>%
  left_join((procedure_codes_op_dates%>%distinct), by=c("PERSON_ID","EPIKEY","CODE", "OPERTN_POSITION"="POSITION"))


rm(procedure_codes_op_dates)
rm(procedure_codes)

procedure_codes = 
procedure_codes_joined %>%
  mutate(op_date_type = ifelse(is.na(OPDATE),0,1)) %>%
  mutate(OPERATION_DATE = ifelse(is.na(OPDATE),EPISTART,OPDATE)) %>%
  select(-OPDATE)

rm(procedure_codes_joined)

procedure_codes = procedure_codes %>%
  mutate(OPERATION_DATE=as.Date(OPERATION_DATE, origin = "1970-01-01"))


main_first_simple = main_cohort_final_first %>%
  select(PERSON_ID, OPERATION_DATE, surgery_type, combination)


main_first_codes = main_first_simple %>%
  filter(surgery_type=="savr") %>%
  left_join((procedure_codes%>%select(CODE,PERSON_ID,OPERATION_DATE)),by=c('PERSON_ID','OPERATION_DATE'))


main_first_codes %>% write_rds("tmp/main_first_codes.rds")

```


```{r, savr_cases_plot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE,fig.width=12, fig.height=5}

main_first_codes = read_rds("tmp/main_first_codes.rds")

main_first_codes_wide = main_first_codes %>%
  pivot_wider(names_from = CODE, 
              values_from = CODE, 
              values_fn = list(CODE = ~1), values_fill = list(CODE = 0))

count_combinations = main_first_codes %>%
  distinct(PERSON_ID,CODE) %>%
  group_by(PERSON_ID) %>%
  summarize(COMBINED = paste(CODE, collapse = " + ")) %>%
  count(COMBINED) %>%
  arrange(desc(n)) %>%
    mutate(
    CumulativeCount = cumsum(n),
    CumulativePercentage = (CumulativeCount / sum(n)) * 100
  ) %>%
  mutate(Percentage = n/sum(n)) %>%
  mutate(repair = ifelse(str_detect(COMBINED,"K265|K268|K269"),1,0))
  

percent_share = count_combinations %>% 
  head(20) %>% tail(1) %>% pull(CumulativePercentage)

count_combinations %>% 
  mutate(COMBINED = factor(COMBINED, levels = COMBINED[order(-n)])) %>%
  mutate(Percentage = Percentage*100) %>%
  head(20) %>%
  ggplot(aes(x = COMBINED, y = Percentage)) +
  geom_bar(stat = "identity", fill="#7346CB") +
  labs(
    x = "Code Combinations",
    y = "%",
    title = paste0("Top 20 Code Combinations (accounting for ",round(percent_share,2), "% of cases) \ncontributing to SAVR cases")
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
        panel.grid.major = element_blank(), 
        panel.grid.minor = element_blank(),
        panel.background = element_blank())




repair_codes = round((count_combinations %>% filter(repair==1) %>% summarise(n=sum(n))) /(count_combinations %>% summarise(n=sum(n)))*100,2)

repair_codes_remove = round((count_combinations %>% filter(COMBINED%in%c("K265","K268","K269")) %>% summarise(n=sum(n))) /(count_combinations %>% summarise(n=sum(n)))*100,2)


```


`r repair_codes`% of SAVR code combinations include a Repair code. Removing repair codes would remove `r repair_codes_remove`% of SAVR cases.

## What combinations are driving TAVI cases?

```{r, tavi_cases_plot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE,fig.width=12, fig.height=8}

main_first_simple = main_cohort_final_first %>%
  select(PERSON_ID, OPERATION_DATE, surgery_type, combination)


tavi_count_combinations = main_first_simple %>%
  filter(surgery_type=="tavi") %>%
  unnest(combination) %>%
  distinct(PERSON_ID,combination) %>%
  group_by(PERSON_ID) %>%
  summarize(COMBINED = paste(combination, collapse = " + ")) %>%
  count(COMBINED) %>%
  arrange(desc(n)) %>%
    mutate(
    CumulativeCount = cumsum(n),
    CumulativePercentage = (CumulativeCount / sum(n)) * 100
  ) %>%
  mutate(Percentage = n/sum(n)) %>%
  mutate(COMBINED = str_replace(COMBINED,"tavi1","TAVI 1 (surgical)")) %>%
  mutate(COMBINED = str_replace(COMBINED,"tavi2","TAVI 2 (surgical)")) %>%
  mutate(COMBINED = str_replace(COMBINED,"tavi3","TAVI 3 (transluminal)")) %>%
  mutate(COMBINED = str_replace(COMBINED,"tavi4","TAVI 4 (transluminal)"))



tavi_count_combinations %>% 
  mutate(COMBINED = factor(COMBINED, levels = COMBINED[order(-n)])) %>%
  mutate(Percentage = Percentage*100) %>%
  head(20) %>%
  ggplot(aes(x = COMBINED, y = Percentage)) +
  geom_bar(stat = "identity", fill="#7346CB") +
  labs(
    x = "TAVI Combinations",
    y = "%",
    title = paste0("TAVI Combinations contributing to TAVI cases")
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
        panel.grid.major = element_blank(), 
        panel.grid.minor = element_blank(),
        panel.background = element_blank())


```




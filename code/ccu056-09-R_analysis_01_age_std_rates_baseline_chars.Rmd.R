---
title: "CCU056 Analysis"
subtitle: "Analysis 01 - Age Standardised Rates and Baseline Characteristics"
output:
  bookdown::html_document2:
    number_sections: true
css: style.css
---


```{r, setup, warning=FALSE, error=FALSE, message=FALSE,eval=TRUE, echo=FALSE}


#libraries
library(odbc)
library(DBI)
library(tidyverse)
library(glue)
library(kableExtra)

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



```{r, parameters, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

study_start_date = "2020-01-01"


tavi_colour = "#ff0030"
savr_colour = "#b5e3eb"


analysis_cohort = read_rds("tmp/main_cohort_final_first_new3.rds")


esp = dbGetQuery(con,sprintf(glue(select_all_proj,"esp")))
esp_groups = dbGetQuery(con,sprintf(glue(select_all_proj,"esp_groups")))

analysis_cohort =
analysis_cohort %>%
    mutate(treat = case_when(
    OPERATION_YEAR >=2000 & OPERATION_YEAR <=2004 ~ "2000-2004",
    OPERATION_YEAR >=2005 & OPERATION_YEAR <=2009 ~ "2005-2009", 
    OPERATION_YEAR >=2010 & OPERATION_YEAR <=2014 ~ "2010-2014", 
    OPERATION_YEAR >=2015 & OPERATION_YEAR <=2019 ~ "2015-2019",
    OPERATION_YEAR >=2020 & OPERATION_YEAR <=2023 ~ "2020-2023",
    TRUE ~ NA_character_))


```


# Age-standardised annual procedure rates

Age standardised annual SAVR/TAVI procedures rates for different sex, ethnicity and SES groups, will be estimated using denominator census data.

- Using 2021 Census population data
- Used the European Standard Population - derived a smoothed single-year age distribution from data presented in 5-year age groups. Specifically used Beers interpolation ordinary method. Ordinary method conserves sums in 5-year age groups, whereas Modified does some smoothing between 5-year age groups too, and is not constrained.
- The rates presented for each time period are averaged by the number of years in that time period.



```{r, parametersPop, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

pop_lsoa_mapped_quintile_age = read_rds("tmp/pop_lsoa_mapped_quintile_age.rds")
pop_lsoa_mapped_quintile_sex_age = read_rds("tmp/pop_lsoa_mapped_quintile_sex_age.rds")
pop_lsoa_mapped_sex_age = read_rds("tmp/pop_lsoa_mapped_sex_age.rds")

pop_ethnicity_mapped_ethnicity_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_age.rds")
pop_ethnicity_mapped_sex_age = read_rds("tmp/pop_ethnicity_mapped_sex_age.rds")
pop_ethnicity_mapped_ethnicity_sex_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_sex_age.rds")
pop_ethnicity_mapped_ethnicity_high_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_high_age.rds")
pop_ethnicity_mapped_ethnicity_high_sex_age = read_rds("tmp/pop_ethnicity_mapped_ethnicity_high_sex_age.rds")


# Using the mid population denominator only
pop_lsoa_mapped_quintile_age = pop_lsoa_mapped_quintile_age %>% filter(YEAR==2011) %>%
    mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(QUINTILE,age_group,YEAR) %>% summarise(pop_lsoa_var_quintile_age= sum(pop_lsoa_var_quintile_age))

pop_lsoa_mapped_quintile_sex_age = pop_lsoa_mapped_quintile_sex_age %>% filter(YEAR==2011) %>%
    mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(QUINTILE,sex,age_group,YEAR) %>% summarise(pop_lsoa_var_quintile_sex_age= sum(pop_lsoa_var_quintile_sex_age))

pop_lsoa_mapped_sex_age = pop_lsoa_mapped_sex_age %>% filter(YEAR==2011) %>%
    mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(sex,age_group,YEAR) %>% summarise(pop_lsoa_var_sex_age= sum(pop_lsoa_var_sex_age))

pop_ethnicity_mapped_ethnicity_age = pop_ethnicity_mapped_ethnicity_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_sex_age = pop_ethnicity_mapped_sex_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_ethnicity_sex_age = pop_ethnicity_mapped_ethnicity_sex_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_ethnicity_high_age = pop_ethnicity_mapped_ethnicity_high_age %>% filter(YEAR==2011)
pop_ethnicity_mapped_ethnicity_high_sex_age = pop_ethnicity_mapped_ethnicity_high_sex_age %>% filter(YEAR==2011)


```


```{r, esp, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

ESP = 
esp_groups %>%
  select(age_group,population_ord_group) %>%
  add_row(age_group=c(">=90"),population_ord_group=c(1000)) %>%
  mutate(age_group=ifelse(age_group %in% c("85-89",">=90"),
                          "85+",
                          age_group)) %>%
  group_by(age_group) %>% summarise(population_ord_group=sum(population_ord_group)) %>%
  mutate(prop=population_ord_group/sum(population_ord_group)) %>% select(-population_ord_group)

```



```{r, adhocEth, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}


# adhoc investigation

# swtiching from the 2021 pop denominator to the 2011 significantly changed the rate of the the Other Ethnic groups


pop_lsoa_mapped_quintile_age_ah = read_rds("tmp/pop_lsoa_mapped_quintile_age.rds")
pop_lsoa_mapped_quintile_sex_age_ah = read_rds("tmp/pop_lsoa_mapped_quintile_sex_age.rds")
pop_lsoa_mapped_sex_age_ah = read_rds("tmp/pop_lsoa_mapped_sex_age.rds")

pop_ethnicity_mapped_ethnicity_age_ah = read_rds("tmp/pop_ethnicity_mapped_ethnicity_age.rds")
pop_ethnicity_mapped_sex_age_ah = read_rds("tmp/pop_ethnicity_mapped_sex_age.rds")
pop_ethnicity_mapped_ethnicity_sex_age_ah = read_rds("tmp/pop_ethnicity_mapped_ethnicity_sex_age.rds")
pop_ethnicity_mapped_ethnicity_high_age_ah = read_rds("tmp/pop_ethnicity_mapped_ethnicity_high_age.rds")
pop_ethnicity_mapped_ethnicity_high_sex_age_ah = read_rds("tmp/pop_ethnicity_mapped_ethnicity_high_sex_age.rds")


analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  filter(!age_group %in% c("85-89",">=90")) %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,age_group) %>% count() %>% 
  
  left_join((pop_ethnicity_mapped_ethnicity_high_age_ah%>% filter(YEAR==2011)),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,ETHNIC_DESC_HIGH_ONLY,age_group,weighted_rate) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY) %>% summarise(asr = sum(weighted_rate)) %>%
  mutate(pop_year="2011") %>%
  rbind(analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(!age_group %in% c("85-89",">=90")) %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,age_group) %>% count() %>% 
  
  left_join((pop_ethnicity_mapped_ethnicity_high_age_ah%>% filter(YEAR==2021)),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,ETHNIC_DESC_HIGH_ONLY,age_group,weighted_rate) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY) %>% summarise(asr = sum(weighted_rate)) %>% 
    mutate(pop_year="2021")) %>%
  #filter(year==2020) %>%
  pivot_wider(names_from=pop_year,values_from=asr) %>%
  mutate(ratio=`2011`/`2021`)


pop_ethnicity_mapped_ethnicity_high_age_ah %>%
  group_by(project_high,YEAR) %>% summarise(pop_ethnicity_var_ethnicity_high_age=sum(pop_ethnicity_var_ethnicity_high_age)) %>%
  #filter(project_high=="Other ethnic groups") %>%
  pivot_wider(names_from=YEAR,values_from=pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(ratio=`2021`/`2011`)





#####################################


analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  filter(!age_group %in% c("85-89",">=90")) %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,age_group) %>% count() %>% 
  
  left_join((pop_ethnicity_mapped_ethnicity_high_age_ah%>% filter(YEAR==2011)),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,ETHNIC_DESC_HIGH_ONLY,age_group,weighted_rate) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY) %>% summarise(asr = sum(weighted_rate)) %>%
  mutate(pop_year="2011")



analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(!age_group %in% c("85-89",">=90")) %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,age_group) %>% count() %>% 
  
  left_join((pop_ethnicity_mapped_ethnicity_high_age_ah%>% filter(YEAR==2021)),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  filter(ETHNIC_DESC_HIGH_ONLY=="Other ethnic groups") %>%
  ungroup() %>% group_by(year) %>%
  summarise(n=sum(n),
            pop_2021=sum(pop_ethnicity_var_ethnicity_high_age)) %>%
  left_join((

analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(!age_group %in% c("85-89",">=90")) %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,age_group) %>% count() %>% 
  
  left_join((pop_ethnicity_mapped_ethnicity_high_age_ah%>% filter(YEAR==2011)),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  filter(ETHNIC_DESC_HIGH_ONLY=="Other ethnic groups") %>%
  ungroup() %>% group_by(year) %>%
  summarise(n=sum(n),
            pop_2011=sum(pop_ethnicity_var_ethnicity_high_age)) %>% select(-n)
)) %>%
  
  mutate(rate_2021=n/pop_2021,rate_2011=n/pop_2011)



```






## Sex

```{r, asSexPlotWorking, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

# as_sex = analysis_cohort %>%
#   filter(age_group!="Unknown") %>%
#   filter(SEX%in%c("Male","Female")) %>%
#   select(PERSON_ID,OPERATION_DATE,SEX,age,age_group) %>%
#   mutate(year=year(OPERATION_DATE)) %>%
#   group_by(year,SEX,age_group) %>% count() %>%
#   left_join(pop_ethnicity_mapped_sex_age,by = join_by(SEX, age_group, POP_YEAR_TYPE)) %>%
#   filter(!is.na(pop_ethnicity_var_sex_age)) %>%
#   mutate(crude_rate=n/pop_ethnicity_var_sex_age) %>%
#   mutate(crude_rate_100000 = crude_rate*100000) %>% 
#   left_join(ESP) %>%
#   mutate(weighted_rate = prop*crude_rate_100000) %>%
#   select(year,SEX,age_group,weighted_rate) %>%
#   group_by(year,SEX) %>% summarise(asr = sum(weighted_rate))
# 



as_sex = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(SEX%in%c("Male","Female")) %>%
  select(PERSON_ID,OPERATION_DATE,SEX,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,SEX,age_group) %>% count() %>%
  left_join((pop_ethnicity_mapped_sex_age),by = join_by(SEX, age_group)) %>%
  filter(!is.na(pop_ethnicity_var_sex_age)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_sex_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,SEX,age_group,weighted_rate) %>%
  group_by(year,SEX) %>% summarise(asr = sum(weighted_rate))


as_sex_surgery = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(SEX%in%c("Male","Female")) %>%
  select(PERSON_ID,OPERATION_DATE,SEX,age,age_group,surgery_type) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,SEX,age_group,surgery_type) %>% count() %>%
  left_join((pop_ethnicity_mapped_sex_age),by = join_by(SEX, age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_sex_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,SEX,age_group,weighted_rate,surgery_type) %>%
  group_by(year,SEX,surgery_type) %>% summarise(asr = sum(weighted_rate))
  


```


```{r, asSexPlot, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE, fig.height=7, fig.width=12, fig.cap="Age standardised rates for Sex"}

y_axis_sex_savr = 45
y_axis_sex_savr_text = 47

as_sex_plot = 
  ggplot((as_sex
          ),
       aes(x = year, y = asr, group = SEX, color=SEX)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate per 100,000 people"
       , title = "Age standardised procedure rates per 100,000 people by year and sex, England, 2000 to 2023"
       ) +
  #scale_x_discrete(breaks=break_years,labels=label_years) +

  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
        legend.position = c(0.02, 0.86),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22")) +

  scale_x_continuous(labels=c(seq(2000,2023,1)),breaks=c(seq(2000,2023,1))) +
  
  
  geom_segment(aes(x= 2000, xend=2004, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2004, xend=2000, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2005, xend=2009, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2009, xend=2005, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2010, xend=2014, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2014, xend=2010, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2015, xend=2019, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2019, xend=2015, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2020, xend=2023, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  
  geom_text(aes(x = 2000.6, y = y_axis_sex_savr_text, label = "2000-2004"), hjust = -0.3, color = "#475ca6") +
  geom_text(aes(x = 2005.3, y = y_axis_sex_savr_text, label = "2005-2009"), hjust = -0.5, color = "#475ca6") +
  geom_text(aes(x = 2010.3, y = y_axis_sex_savr_text, label = "2010-2014"), hjust = -0.5, color = "#475ca6") +
  geom_text(aes(x = 2015.3, y = y_axis_sex_savr_text, label = "2015-2019"), hjust = -0.5, color = "#475ca6") +
  geom_text(aes(x = 2019.8, y = y_axis_sex_savr_text, label = "2020-2023"), hjust = -0.5, color = "#475ca6") 

as_sex_plot
ggsave("Plots/as_sex_plot.png")

```



```{r, asSexSurgeryPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.height=7, fig.width=12, fig.cap="Age standardised rates for Sex by Surgery Type"}

y_axis_sex_savr = 32
y_axis_sex_savr_text = 35

as_sex_surgery_plot = ggplot((as_sex_surgery %>%
                                mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI"))
          ),
       aes(x = year, y = asr, group = SEX, color=SEX)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate per 100,000 people"
       #, title = "Age standardised procedure rates per 100,000 people by year and sex, England, 2000 to 2023"
       ) +
  #scale_x_discrete(breaks=break_years,labels=label_years) +
  facet_wrap(~surgery_type, scales = "free_y", ncol = 1) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
      strip.text = element_text(
    size = 14),
    axis.text.x = element_text(angle = 90, hjust = 1, size=12),
    axis.text.y = element_text(size=12),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
    legend.text=element_text(size=14),
        legend.position = c(0.02, 0.91),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"),size=16)
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22")) +

scale_x_continuous(labels=c(seq(2000,2023,1)),breaks=c(seq(2000,2023,1))) +
  
  
  geom_segment(aes(x= 2000, xend=2004, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2004, xend=2000, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2005, xend=2009, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2009, xend=2005, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2010, xend=2014, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2014, xend=2010, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2015, xend=2019, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2019, xend=2015, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2020, xend=2023, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  
  geom_text(aes(x = 2000.6, y = y_axis_sex_savr_text, label = "2000-2004"), hjust = -0.15, color = "#475ca6",size=5) +
  geom_text(aes(x = 2005.3, y = y_axis_sex_savr_text, label = "2005-2009"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2010.3, y = y_axis_sex_savr_text, label = "2010-2014"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2015.3, y = y_axis_sex_savr_text, label = "2015-2019"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2019.8, y = y_axis_sex_savr_text, label = "2020-2023"), hjust = -0.22, color = "#475ca6",size=5)




#here

ggplot2::ggsave(filename = "/db-mnt/databricks/rstudio_collab/CCU056/Plots/sex.png", plot = as_sex_surgery_plot,
                dpi = 1100,
                width = 12, height = 7, 
                units = "in")


```




```{r}

# Overall
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


pop_ethnicity_mapped_overall_age = 
pop_ethnicity_2011_mapped_overall_age

as_overall_surgery = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  #filter(SEX%in%c("Male","Female")) %>%
  select(PERSON_ID,OPERATION_DATE,SEX,age,age_group,surgery_type) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,age_group,surgery_type) %>% count() %>%
  left_join((pop_ethnicity_mapped_overall_age),by = join_by(age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_overall_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,age_group,weighted_rate,surgery_type) %>%
  group_by(year,surgery_type) %>% summarise(asr = sum(weighted_rate))

y_axis_sex_savr = 32
y_axis_sex_savr_text = 35

as_overall_surgery_plot = ggplot((as_overall_surgery %>%
                                mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI"))
          ),
       aes(x = year, y = asr, color="#C00000")) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate per 100,000 people"
       #, title = "Age standardised procedure rates per 100,000 people by year and sex, England, 2000 to 2023"
       ) +
  #scale_x_discrete(breaks=break_years,labels=label_years) +
  facet_wrap(~surgery_type, scales = "free_y", ncol = 1) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
      strip.text = element_text(
    size = 14),
    axis.text.x = element_text(angle = 90, hjust = 1, size=12),
    axis.text.y = element_text(size=12),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.position = "none",

    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"),size=16)
    ) +
  guides(color = guide_legend(ncol = 1)) +


scale_x_continuous(labels=c(seq(2000,2023,1)),breaks=c(seq(2000,2023,1))) +
  
  
  geom_segment(aes(x= 2000, xend=2004, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2004, xend=2000, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2005, xend=2009, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2009, xend=2005, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2010, xend=2014, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2014, xend=2010, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2015, xend=2019, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2019, xend=2015, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2020, xend=2023, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  
  geom_text(aes(x = 2000.6, y = y_axis_sex_savr_text, label = "2000-2004"), hjust = -0.15, color = "#475ca6",size=5) +
  geom_text(aes(x = 2005.3, y = y_axis_sex_savr_text, label = "2005-2009"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2010.3, y = y_axis_sex_savr_text, label = "2010-2014"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2015.3, y = y_axis_sex_savr_text, label = "2015-2019"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2019.8, y = y_axis_sex_savr_text, label = "2020-2023"), hjust = -0.22, color = "#475ca6",size=5)




#here

ggplot2::ggsave(filename = "/db-mnt/databricks/rstudio_collab/CCU056/Plots/overall.png", plot = as_overall_surgery_plot,
                dpi = 1100,
                width = 12, height = 7, 
                units = "in")



as_overall_surgery_plot

```



```{r, asSexTable, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.height=7, fig.width=12}



as_sex_surgery_time = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(SEX%in%c("Male","Female")) %>%
  select(PERSON_ID,OPERATION_DATE,SEX,age,age_group,surgery_type,treat) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,treat,SEX,age_group,surgery_type) %>% count() %>%
  left_join((pop_ethnicity_mapped_sex_age),by = join_by(SEX, age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_sex_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,treat,SEX,age_group,weighted_rate,surgery_type) %>%
  group_by(year,treat,SEX,surgery_type) %>% summarise(asr = sum(weighted_rate)) %>%
  ungroup() %>% group_by(treat,SEX,surgery_type) %>% summarise(asr=mean(asr))


as_sex_plot_table_savr = as_sex_surgery_time %>%
  mutate(asr=round(asr,2)) %>%
  filter(surgery_type=="savr") %>% select(-surgery_type) %>%
  pivot_wider(names_from=treat,values_from=asr) %>%
  rename(Sex=SEX)

as_sex_plot_table_tavi = as_sex_surgery_time %>%
  mutate(asr=round(asr,2)) %>%
  filter(surgery_type=="tavi") %>% select(-surgery_type) %>%
  pivot_wider(names_from=treat,values_from=asr) %>%
  rename(Sex=SEX)

# as_sex_plot_table_savr = as_sex_surgery %>%
#   mutate(asr=round(asr,2)) %>%
#   filter(surgery_type=="savr") %>% select(-surgery_type) %>%
#   pivot_wider(names_from=year,values_from=asr) %>%
#   rename(Sex=SEX)
# 
# as_sex_plot_table_tavi = as_sex_surgery %>%
#   mutate(asr=round(asr,2)) %>%
#   filter(surgery_type=="tavi") %>% select(-surgery_type) %>%
#   pivot_wider(names_from=year,values_from=asr) %>%
#   rename(Sex=SEX)


# p2 <- gridExtra::tableGrob(as_sex_plot_table)
# # Set widths/heights to 'fill whatever space I have'
# p2$widths <- unit(rep(1, ncol(p2)), "null")
# p2$heights <- unit(rep(1, nrow(p2)), "null")
# 
# # Format table as plot
# p3 <- ggplot() +
#   annotation_custom(p2)
# 
# p3


kableExtra::kbl(
  as_sex_plot_table_savr,
    booktabs = T, 
    caption=("Average Age standardised rates for Sex - SAVR"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

kableExtra::kbl(
  as_sex_plot_table_tavi%>%mutate("2000-2004"="-")%>%relocate("2000-2004",.before="2005-2009"),
    booktabs = T, 
    caption=("Average Age standardised rates for Sex - TAVI"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )



```


## Ethnicity


```{r, asEthHighPlotWorking, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

as_eth_high = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,age_group) %>% count() %>% 
  
  left_join((pop_ethnicity_mapped_ethnicity_high_age),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,ETHNIC_DESC_HIGH_ONLY,age_group,weighted_rate) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY) %>% summarise(asr = sum(weighted_rate))



as_eth_high_surgery = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group,surgery_type) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,age_group,surgery_type) %>% count() %>%
  left_join((pop_ethnicity_mapped_ethnicity_high_age),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,ETHNIC_DESC_HIGH_ONLY,age_group,weighted_rate,surgery_type) %>%
  group_by(year,ETHNIC_DESC_HIGH_ONLY,surgery_type) %>% summarise(asr = sum(weighted_rate))



```



```{r, asEthHighPlot, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE, fig.height=7, fig.width=12, fig.cap="Age standardised rates for Ethnicity"}



ggplot((as_eth_high
          ),
       aes(x = year, y = asr, group = ETHNIC_DESC_HIGH_ONLY, color=ETHNIC_DESC_HIGH_ONLY)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate \nper 100,000 people"
       ,title = "Age standardised procedure rates per 100,000 people by year and ethnicity, England, 2000 to 2023"
       ) +

  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.80),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(values = c("#FF042E", "#3CB08C", "#FF5100", "#7346CB", "#FFCD16")) +

  
  geom_vline(xintercept = 2005, linetype = "dashed", color = "gray70") +
  geom_vline(xintercept = 2015, linetype = "dashed", color = "gray70") +
  geom_vline(xintercept = 2020, linetype = "dashed", color = "gray70") +
  
  #coord_cartesian(xlim = c(2000,2023),clip = "off")
  
  geom_segment(aes(x= 2000, xend=2005, y=28, yend=28), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2005, xend=2000, y=28, yend=28), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2015, xend=2020, y=28, yend=28), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2020, xend=2015, y=28, yend=28), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2000, xend=2020, y=30, yend=30), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2020, xend=2000, y=30, yend=30), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2020, xend=2023, y=28, yend=28), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=28, yend=28), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2020, xend=2023, y=30, yend=30), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=30, yend=30), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  
  geom_text(aes(x = 2008, y = 32, label = "Prior to Pandemic"), hjust = -0.3, color = "#475ca6") +
  geom_text(aes(x = 2020, y = 32, label = "Pandemic"), hjust = -0.5, color = "#475ca6") +
  
  geom_text(aes(x = 2000.8, y = 29, label = "Beginning"), hjust = -0.5, color = "#475ca6") +
  geom_text(aes(x = 2017, y = 29, label = "End"), hjust = -0.2, color = "#475ca6") +
  geom_text(aes(x = 2021, y = 29, label = "During"), hjust = -0.05, color = "#475ca6")


```



```{r, asEthHighSurgeryPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.height=7, fig.width=12, fig.cap="Age standardised rates for Ethnicity"}

y_axis_sex_savr = 25
y_axis_sex_savr_text = 27

eth_plot = ggplot((as_eth_high_surgery %>%
                                mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI"))
          ),
       aes(x = year, y = asr, group = ETHNIC_DESC_HIGH_ONLY, color=ETHNIC_DESC_HIGH_ONLY)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate per 100,000 people"
      # ,title = "Age standardised procedure rates per 100,000 people by year and ethnicity, England, 2000 to 2023"
       ) +

  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
      strip.text = element_text(
    size = 14),
    axis.text.x = element_text(angle = 90, hjust = 1, size=12),
    axis.text.y = element_text(size=12),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
    legend.text=element_text(size=14),
        legend.position = c(0.02, 0.91),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"),size=16)
    ) +
  guides(color = guide_legend(ncol = 1)) +
  facet_wrap(~surgery_type, scales = "free_y", ncol = 1) +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100")) +

scale_x_continuous(labels=c(seq(2000,2023,1)),breaks=c(seq(2000,2023,1))) +
  
  
  geom_segment(aes(x= 2000, xend=2004, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2004, xend=2000, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2005, xend=2009, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2009, xend=2005, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2010, xend=2014, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2014, xend=2010, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2015, xend=2019, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2019, xend=2015, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2020, xend=2023, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  

  geom_text(aes(x = 2000.6, y = y_axis_sex_savr_text, label = "2000-2004"), hjust = -0.15, color = "#475ca6",size=5) +
  geom_text(aes(x = 2005.3, y = y_axis_sex_savr_text, label = "2005-2009"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2010.3, y = y_axis_sex_savr_text, label = "2010-2014"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2015.3, y = y_axis_sex_savr_text, label = "2015-2019"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2019.8, y = y_axis_sex_savr_text, label = "2020-2023"), hjust = -0.22, color = "#475ca6",size=5)




ggplot2::ggsave(filename = "/db-mnt/databricks/rstudio_collab/CCU056/Plots/ethnicity.png", plot = eth_plot,
                dpi = 1100,
                width = 12, height = 7, 
                units = "in")




```

```{r, asEthnicityTable, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.height=7, fig.width=12}



as_eth_high_surgery_time = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(ETHNIC_DESC_HIGH_ONLY!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_HIGH_ONLY,age,age_group,surgery_type,treat) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,treat,ETHNIC_DESC_HIGH_ONLY,age_group,surgery_type) %>% count() %>%
  left_join((pop_ethnicity_mapped_ethnicity_high_age),
            by = join_by("ETHNIC_DESC_HIGH_ONLY"=="project_high", age_group)) %>%
  mutate(crude_rate=n/pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,treat,ETHNIC_DESC_HIGH_ONLY,age_group,weighted_rate,surgery_type) %>%
  group_by(year,treat,ETHNIC_DESC_HIGH_ONLY,surgery_type) %>% summarise(asr = sum(weighted_rate)) %>%
  ungroup() %>% group_by(treat,ETHNIC_DESC_HIGH_ONLY,surgery_type) %>% summarise(asr=mean(asr))


as_eth_plot_table_savr = as_eth_high_surgery_time %>%
  mutate(asr=round(asr,2)) %>%
  filter(surgery_type=="savr") %>% select(-surgery_type) %>%
  pivot_wider(names_from=treat,values_from=asr) %>%
  rename(Ethnicity=ETHNIC_DESC_HIGH_ONLY)

as_eth_plot_table_tavi = as_eth_high_surgery_time %>%
  mutate(asr=round(asr,2)) %>%
  filter(surgery_type=="tavi") %>% select(-surgery_type) %>%
  pivot_wider(names_from=treat,values_from=asr) %>%
  rename(Ethnicity=ETHNIC_DESC_HIGH_ONLY)




kableExtra::kbl(
  as_eth_plot_table_savr,
    booktabs = T, 
    caption=("Average Age standardised rates for Ethnicity - SAVR"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

kableExtra::kbl(
  as_eth_plot_table_tavi%>%mutate("2000-2004"="-")%>%relocate("2000-2004",.before="2005-2009"),
    booktabs = T, 
    caption=("Average Age standardised rates for Ethnicity - TAVI"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

```

### Detailed

```{r, asEthPlotWorking, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

as_eth = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(ETHNIC_DESC_DETAIL2!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,ETHNIC_DESC_DETAIL2,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,ETHNIC_DESC_DETAIL2,age_group) %>% count() %>%
  left_join((pop_ethnicity_mapped_ethnicity_age),by = c("ETHNIC_DESC_DETAIL2", "age_group")) %>%
  mutate(crude_rate=n/pop_ethnicity_var_ethnicity_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,ETHNIC_DESC_DETAIL2,age_group,weighted_rate) %>%
  group_by(year,ETHNIC_DESC_DETAIL2) %>% summarise(asr = sum(weighted_rate)) %>%
  mutate(type=ifelse(ETHNIC_DESC_DETAIL2%in%c("Any other Asian background",
"Bangladeshi",
"Indian",
"Pakistani"),"Asian or Asian British",
ifelse(ETHNIC_DESC_DETAIL2%in%c("African",
"Any other Black background",
"Caribbean"),"Black or Black British",
ifelse(ETHNIC_DESC_DETAIL2%in%c("Any other Mixed background",
"White and Asian",
"White and Black African",
"White and Black Caribbean"),"Mixed or Multiple ethnic groups",
ifelse(ETHNIC_DESC_DETAIL2%in%c("Any other ethnic group",
"Chinese"),"Other ethnic groups",
ifelse(ETHNIC_DESC_DETAIL2%in%c("Any other White background",
"British",
"Irish"),"White",
"NA"))))))


```

```{r, asEthPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.height=12, fig.width=12, fig.cap="Age standardised rates for Ethnicity detail"}

custom_colors <- c("#FF042E", "#3CB08C", "#FF5100", "#7346CB", "#FFCD16", "grey") #"#B4E4EA"

plot1 = ggplot((as_eth%>%filter(type=="Asian or Asian British")
          # mutate(ETHNIC_DESC_HIGH_ONLY=factor(ETHNIC_DESC_HIGH_ONLY,
          #                                     levels=c(
          #                                       "Asian or Asian British",
          #                                       "Black or Black British",
          #                                       "Mixed or Multiple ethnic groups",
          #                                       "Other ethnic groups",
          #                                       "White",
          #                                       "Unknown"
          #                                     )))
          ),
       aes(x = year, y = asr, group = ETHNIC_DESC_DETAIL2, color=ETHNIC_DESC_DETAIL2, linetype=ETHNIC_DESC_DETAIL2)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate \nper 100,000 people"
       ) +

  scale_y_continuous(labels = scales::comma) +
  facet_wrap(~type, scales = "free", ncol = 1) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(values = c("#FF042E","#7D0217", "#FF0472","#FF8C8C"))

plot2 = ggplot((as_eth%>%filter(type=="Black or Black British")
          # mutate(ETHNIC_DESC_HIGH_ONLY=factor(ETHNIC_DESC_HIGH_ONLY,
          #                                     levels=c(
          #                                       "Asian or Asian British",
          #                                       "Black or Black British",
          #                                       "Mixed or Multiple ethnic groups",
          #                                       "Other ethnic groups",
          #                                       "White",
          #                                       "Unknown"
          #                                     )))
          ),
       aes(x = year, y = asr, group = ETHNIC_DESC_DETAIL2, color=ETHNIC_DESC_DETAIL2,linetype=ETHNIC_DESC_DETAIL2)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate \nper 100,000 people"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +

  scale_y_continuous(labels = scales::comma) +
  facet_wrap(~type, scales = "free", ncol = 1) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(values = c("#3CB08C", "#1BFF9E", "#199300"))


plot3 = ggplot((as_eth%>%filter(type=="Mixed or Multiple ethnic groups")
          # mutate(ETHNIC_DESC_HIGH_ONLY=factor(ETHNIC_DESC_HIGH_ONLY,
          #                                     levels=c(
          #                                       "Asian or Asian British",
          #                                       "Black or Black British",
          #                                       "Mixed or Multiple ethnic groups",
          #                                       "Other ethnic groups",
          #                                       "White",
          #                                       "Unknown"
          #                                     )))
          ),
       aes(x = year, y = asr, group = ETHNIC_DESC_DETAIL2, color=ETHNIC_DESC_DETAIL2,linetype=ETHNIC_DESC_DETAIL2)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate \nper 100,000 people"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +

  scale_y_continuous(labels = scales::comma) +
  facet_wrap(~type, scales = "free", ncol = 1) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(values = c("#FF5100","#FFA87F","#FF1C00","#FED8B1"))


plot4 = ggplot((as_eth%>%filter(type=="Other ethnic groups")
          # mutate(ETHNIC_DESC_HIGH_ONLY=factor(ETHNIC_DESC_HIGH_ONLY,
          #                                     levels=c(
          #                                       "Asian or Asian British",
          #                                       "Black or Black British",
          #                                       "Mixed or Multiple ethnic groups",
          #                                       "Other ethnic groups",
          #                                       "White",
          #                                       "Unknown"
          #                                     )))
          ),
       aes(x = year, y = asr, group = ETHNIC_DESC_DETAIL2, color=ETHNIC_DESC_DETAIL2,linetype=ETHNIC_DESC_DETAIL2)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate \nper 100,000 people"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +

  scale_y_continuous(labels = scales::comma) +
  facet_wrap(~type, scales = "free", ncol = 1) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(values = c("#7346CB","#DAC7FF"))


plot5 = ggplot((as_eth%>%filter(type=="White")
          # mutate(ETHNIC_DESC_HIGH_ONLY=factor(ETHNIC_DESC_HIGH_ONLY,
          #                                     levels=c(
          #                                       "Asian or Asian British",
          #                                       "Black or Black British",
          #                                       "Mixed or Multiple ethnic groups",
          #                                       "Other ethnic groups",
          #                                       "White",
          #                                       "Unknown"
          #                                     )))
          ),
       aes(x = year, y = asr, group = ETHNIC_DESC_DETAIL2, color=ETHNIC_DESC_DETAIL2,linetype=ETHNIC_DESC_DETAIL2)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate \nper 100,000 people"
       #, title = "Aortic Stenosis Coverage in HES APC"
       ) +

  scale_y_continuous(labels = scales::comma) +
  facet_wrap(~type, scales = "free", ncol = 1) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    legend.title=element_blank(),
    #legend.position = "none",
        legend.position = c(0.02, 0.98),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +
  scale_color_manual(values = c("#FFCD16","#FFDE9B","#FDFF00"))



gridExtra::grid.arrange(plot1, plot2, plot3, plot5, ncol = 1, top="Age standardised procedure rates per 100,000 people by year and ethnicity, England, 2000 to 2023")



```



## Deprivation


```{r, asQuintHighPlotWorking, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

as_lsoa = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(QUINTILE!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,QUINTILE,age,age_group) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,QUINTILE,age_group) %>% count() %>%
  
  left_join((pop_lsoa_mapped_quintile_age%>%mutate(QUINTILE=as.character(QUINTILE))),
            by = join_by(QUINTILE, age_group)) %>%
  mutate(crude_rate=n/pop_lsoa_var_quintile_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,QUINTILE,age_group,weighted_rate) %>%
  group_by(year,QUINTILE) %>% summarise(asr = sum(weighted_rate))
  

as_lsoa_surgery = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(QUINTILE!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,QUINTILE,age,age_group,surgery_type) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,QUINTILE,age_group,surgery_type) %>% count() %>%
  left_join((pop_lsoa_mapped_quintile_age%>%mutate(QUINTILE=as.character(QUINTILE))),
            by = join_by(QUINTILE, age_group)) %>%
  mutate(crude_rate=n/pop_lsoa_var_quintile_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,QUINTILE,age_group,weighted_rate,surgery_type) %>%
  group_by(year,QUINTILE,surgery_type) %>% summarise(asr = sum(weighted_rate))




```



```{r, asQuintPlot, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE, fig.height=7, fig.width=12, fig.cap="Age standardised rates for SES Quintiles"}


as_lsoa_plot = ggplot((as_lsoa
          ),
       aes(x = year, y = asr, group = QUINTILE, color=QUINTILE)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate per 100,000 people"
      # , title = "Age standardised procedure rates per 100,000 people by year and SES quintiles, England, 2000 to 2023"
       ) +
  #scale_x_discrete(breaks=break_years,labels=label_years) +

  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(angle = 90, hjust = 1),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
        legend.position = c(0.02, 0.81),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"))
    ) +
  guides(color = guide_legend(ncol = 1)) +


  
  geom_vline(xintercept = 2005, linetype = "dashed", color = "gray70") +
  geom_vline(xintercept = 2015, linetype = "dashed", color = "gray70") +
  geom_vline(xintercept = 2020, linetype = "dashed", color = "gray70") +
  
  #coord_cartesian(xlim = c(2000,2023),clip = "off")
  
  geom_segment(aes(x= 2000, xend=2005, y=24, yend=24), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2005, xend=2000, y=24, yend=24), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2015, xend=2020, y=24, yend=24), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2020, xend=2015, y=24, yend=24), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2000, xend=2020, y=26, yend=26), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2020, xend=2000, y=26, yend=26), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2020, xend=2023, y=24, yend=24), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=24, yend=24), color = "#475ca6",
               arrow = arrow(angle=30, length=unit(0.02,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2020, xend=2023, y=26, yend=26), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=26, yend=26), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  
  geom_text(aes(x = 2008, y = 27, label = "Prior to Pandemic"), hjust = -0.3, color = "#475ca6") +
  geom_text(aes(x = 2020, y = 27, label = "Pandemic"), hjust = -0.5, color = "#475ca6") +
  
  geom_text(aes(x = 2000.8, y = 25, label = "Beginning"), hjust = -0.5, color = "#475ca6") +
  geom_text(aes(x = 2017, y = 25, label = "End"), hjust = -0.2, color = "#475ca6") +
  geom_text(aes(x = 2021, y = 25, label = "During"), hjust = -0.05, color = "#475ca6") +
  
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793"))



as_lsoa_plot

```



```{r, asQuintSurgeryPlot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.height=7, fig.width=12, fig.cap="Age standardised rates for SES Quintiles"}


y_axis_sex_savr = 21
y_axis_sex_savr_text = 23

as_lsoa_plot = ggplot((as_lsoa_surgery %>%
                                mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI")) %>%
                         mutate(QUINTILE=ifelse(QUINTILE=="1","IMD 1 Most Deprived",QUINTILE)) %>%
                         mutate(QUINTILE=ifelse(QUINTILE=="5","IMD 5 Least Deprived",QUINTILE)) %>%
                         mutate(QUINTILE=ifelse(QUINTILE=="2","IMD 2",QUINTILE)) %>%
                         mutate(QUINTILE=ifelse(QUINTILE=="3","IMD 3",QUINTILE)) %>%
                          mutate(QUINTILE=ifelse(QUINTILE=="4","IMD 4",QUINTILE))
          ),
       aes(x = year, y = asr, group = QUINTILE, color=QUINTILE)) +
  geom_line(size = 1) +
  labs(x = "", y = "Age standarised rate per 100,000 people"
      # , title = "Age standardised procedure rates per 100,000 people by year and SES quintiles, England, 2000 to 2023"
       ) +
  #scale_x_discrete(breaks=break_years,labels=label_years) +

  facet_wrap(~surgery_type, scales = "free_y", ncol = 1) +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(
      strip.text = element_text(
    size = 14),
    axis.text.x = element_text(angle = 90, hjust = 1, size=12),
    axis.text.y = element_text(size=12),
    plot.title.position = "plot",
    plot.margin = margin(10,30,10,30),
    #legend.position = "none",
    legend.text=element_text(size=14),
        legend.position = c(0.02, 0.91),  # Adjust position within the plot
        legend.justification = c(0, 1),
        legend.margin = margin(t = -10, unit = "pt"),
    axis.title.x = element_text(margin = margin(t = 10, unit = "pt")),
    axis.title.y = element_text(margin = margin(r = 10, unit = "pt"),size=16)
    ) +
  guides(color = guide_legend(ncol = 1)) +

  scale_color_manual(name = "", values = c("IMD 1 Most Deprived"="#9D062A",
                                       
                                           "IMD 2"="#E56B42",
                                         
                                           "IMD 3"="#EDD587",
                                          
                                           "IMD 4"="#A0CCDE",
                                       
                                           "IMD 5 Least Deprived"="#333793")) +

  scale_x_continuous(labels=c(seq(2000,2023,1)),breaks=c(seq(2000,2023,1))) +
  
  
  geom_segment(aes(x= 2000, xend=2004, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2004, xend=2000, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2005, xend=2009, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2009, xend=2005, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  geom_segment(aes(x= 2010, xend=2014, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2014, xend=2010, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2015, xend=2019, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2019, xend=2015, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
    
  geom_segment(aes(x= 2020, xend=2023, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  geom_segment(aes(x= 2023, xend=2020, y=y_axis_sex_savr, yend=y_axis_sex_savr), color = "#475ca6", size = 1.3,
               arrow = arrow(angle=30, length=unit(0.025,"npc"), 
                             type = "open")) +
  
  
  geom_text(aes(x = 2000.6, y = y_axis_sex_savr_text, label = "2000-2004"), hjust = -0.15, color = "#475ca6",size=5) +
  geom_text(aes(x = 2005.3, y = y_axis_sex_savr_text, label = "2005-2009"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2010.3, y = y_axis_sex_savr_text, label = "2010-2014"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2015.3, y = y_axis_sex_savr_text, label = "2015-2019"), hjust = -0.22, color = "#475ca6",size=5) +
  geom_text(aes(x = 2019.8, y = y_axis_sex_savr_text, label = "2020-2023"), hjust = -0.22, color = "#475ca6",size=5)



as_lsoa_plot

ggplot2::ggsave(filename = "/db-mnt/databricks/rstudio_collab/CCU056/Plots/deprivation.png", plot = as_lsoa_plot,
                dpi = 1100,
                width = 12, height = 7, 
                units = "in")

#here

```



```{r, asLSOATable, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.height=7, fig.width=12}



as_lsoa_surgery_time = analysis_cohort %>%
  filter(age_group!="Unknown") %>%
  filter(QUINTILE!="Unknown") %>%
  select(PERSON_ID,OPERATION_DATE,QUINTILE,age,age_group,surgery_type,treat) %>%
  mutate(year=year(OPERATION_DATE)) %>%
  group_by(year,treat,QUINTILE,age_group,surgery_type) %>% count() %>%
  left_join((pop_lsoa_mapped_quintile_age%>%mutate(QUINTILE=as.character(QUINTILE))),
            by = join_by(QUINTILE, age_group)) %>%
  mutate(crude_rate=n/pop_lsoa_var_quintile_age) %>%
  mutate(crude_rate_100000 = crude_rate*100000) %>% 
  left_join(ESP) %>%
  mutate(weighted_rate = prop*crude_rate_100000) %>%
  select(year,treat,QUINTILE,age_group,weighted_rate,surgery_type) %>%
  group_by(year,treat,QUINTILE,surgery_type) %>% summarise(asr = sum(weighted_rate)) %>%
  ungroup() %>% group_by(treat,QUINTILE,surgery_type) %>% summarise(asr=mean(asr))



as_lsoa_plot_table_savr = as_lsoa_surgery_time %>%
  mutate(asr=round(asr,2)) %>%
  filter(surgery_type=="savr") %>% select(-surgery_type) %>%
  pivot_wider(names_from=treat,values_from=asr) %>%
  rename(`SES Quintile`=QUINTILE)

as_lsoa_plot_table_tavi = as_lsoa_surgery_time %>%
  mutate(asr=round(asr,2)) %>%
  filter(surgery_type=="tavi") %>% select(-surgery_type) %>%
  pivot_wider(names_from=treat,values_from=asr) %>%
  rename(`SES Quintile`=QUINTILE)


kableExtra::kbl(
  as_lsoa_plot_table_savr,
    booktabs = T, 
    caption=("Average Age standardised rates for SES Quintile - SAVR"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

kableExtra::kbl(
  as_lsoa_plot_table_tavi%>%mutate("2000-2004"="-")%>%relocate("2000-2004",.before="2005-2009"),
    booktabs = T, 
    caption=("Average Age standardised rates for SES Quintile - TAVI"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )

```


# Baseline Characteristics

Baseline characteristics of SAVR/TAVI patients including age, sex, ethnicity, SES, cardiovascular risk factors and co-morbidities will be analysed using descriptive statistics. These will be compared between different sex, ethnicity and SES groups. 

**Methodology Note** - Cardiovascular risk factors and co-morbidities have been derived using secondary care (HES APC ICD10 codes) only. Primary was not considered as GDPPR only contains records and patients for those alive as at 1 Nov 2019 thus for a significant % of the SAVR/TAVI cohort, no primary records will exist. This would then see these risk factors/co-morbidities % increase year on year simply due to the likelihood that if you were treated later you would be more likely to still be alive 1 Nov 2019.

Continuous variables will be expressed as mean +/- standard deviation, or median (interquartile range) and compared with a student’s t-test or Mann-Whitney U/Kruskal-Wallis H test, respectively. Categorical variables will be analysed using chi-square test or Fisher’s exact test as appropriate. Post-hoc corrections will be applied, to adjust for multiple comparisons. All effects will be considered significant at p<0.05.

P-value in each table is the p-value associated with a univariate test for differences in each variable across strata. Chi-square test of independence is used for categorical variables and a t-test/AVOVA for continuous variables.



```{r, continuousVariables, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# Testing for normality

#Anderson-Darling normality test, which works for larger sample sizes
#Shapiro-Wilk can only be used for sample size 3-5000
# 
# If the p-value is greater than a chosen significance level (e.g., 0.05), you would fail to reject the null hypothesis, indicating that your data may come from a normal distribution. However, if the p-value is less than your chosen significance level, you may conclude that the data deviates from a normal distribution.

age_cont = analysis_cohort %>% select(age) %>% filter(!is.na(age)) %>% pull()
# 
# shapiro.test(age_cont)
# 
# install.packages("nortest")
# library(nortest)
# ad.test(data$variable)



qqnorm(age_cont)
qqline(age_cont, col = 2)


```

## Surgery Type

Baseline Characteristics stratified by surgery type


```{r, tableOne, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

library(table1)

analysis_cohort_table1 = analysis_cohort %>%
  rename(CKD=ckd,Diabetes=diabetes,`Heart Failure`=heart_failure,Hypertension=hypertension,
         PAD=pad,`Asthma/COPD`=asthma_copd,`Cerebrovascular Disease`=cerebrovascular_disease,
         `Coronary Artery Disease`=coronary_artery_disease)
  
  
analysis_cohort_table1$SEX <- 
  factor(analysis_cohort_table1$SEX, levels=c("Male", 
                  "Female",
                  "Unknown")) #"Not Specified (Indeterminate)" will be removed
 
analysis_cohort_table1$age_group <- 
  factor(analysis_cohort_table1$age_group,
         levels=c("18-24",
                  "25-29",
                  "30-34",
                  "35-39",
                  "40-44",
                  "45-49",
                  "50-54",
                  "55-59",
                  "60-64",
                  "65-69",
                  "70-74",
                  "75-79",
                  "80-84",
                  "85+",
                  # "85-89",
                  # ">=90",
                  "Unknown"))

# analysis_cohort_table1$treat <- 
#   factor(analysis_cohort_table1$treat,
#          levels=c("PRIOR - Beginning", 
#                   "PRIOR - Middle",
#                   "PRIOR - End",
#                   "PANDEMIC - During"))

analysis_cohort_table1$treat <- 
  factor(analysis_cohort_table1$treat,
         levels=c("2000-2004", 
                  "2005-2009","2010-2014","2015-2019","2020-2023"))

analysis_cohort_table1$surgery_type    <- factor(analysis_cohort_table1$surgery_type,levels=c("savr", "tavi"),
                                           labels=c("SAVR", "TAVI"))

analysis_cohort_table1$QUINTILE <- 
  factor(analysis_cohort_table1$QUINTILE,
         levels=c("1","2","3","4","5","Unknown"))


eth_desc = unique((names(sort(table(analysis_cohort_table1$ETHNIC_DESC_HIGH_ONLY), decreasing = TRUE))))
eth_desc_detail = unique((names(sort(table(analysis_cohort_table1$ETHNIC_DESC_DETAIL2), decreasing = TRUE))))


analysis_cohort_table1$ETHNIC_DESC_HIGH_ONLY <-
factor(analysis_cohort_table1$ETHNIC_DESC_HIGH_ONLY, 
       levels = as.factor(c(eth_desc[-which((eth_desc) == "Unknown")],"Unknown"))
       )


analysis_cohort_table1$ETHNIC_DESC_DETAIL2 <-
factor(analysis_cohort_table1$ETHNIC_DESC_DETAIL2, 
       levels = as.factor(c(eth_desc_detail[-which((eth_desc_detail) == "Unknown")],"Unknown"))
       )



label(analysis_cohort_table1$SEX)       <- "Sex"
label(analysis_cohort_table1$age)       <- "Age"
label(analysis_cohort_table1$age_group)       <- "Age group"
label(analysis_cohort_table1$ETHNIC_DESC_HIGH_ONLY)       <- "Ethnicity (High-level only)"
label(analysis_cohort_table1$ETHNIC_DESC_DETAIL2)       <- "Ethnicity (Detailed)"
# label(analysis_cohort_table1$DECILE)       <- "IMD Deciles"
label(analysis_cohort_table1$QUINTILE)       <- "IMD Quintiles"

units(analysis_cohort_table1$age)       <- "years"
units(analysis_cohort_table1$age_group)       <- "years"



my.render.cont <- function(x) {
    with(stats.default(x), 
         c("",
           
          "Mean (SD)" = sprintf("%s (%s)",
                                round_pad(MEAN, 1),
                                round_pad(SD, 1)),
         
          "Median (Min, Max)" = sprintf("%s (%s, %s)",
                                       round_pad(MEDIAN, 1), 
                                       round_pad(MIN, 1), 
                                       round_pad(MAX, 1)))
    )
}


my.render.cat <- function(x) {
    c("", sapply(stats.default(x),
                 function(y) with(y,sprintf("%d (%0.1f %%)", if(FREQ>10){round(FREQ / 5) * 5}else{10}, PCT))
                 ))
}

my.render.strat = function(label, n, ...) {
  fg <- unlist(n)
  result <- ifelse(is.na(fg),
                   "P-value", 
                   sprintf("<span class='stratlabel'>%s<br><span class='stratn'>(N=%s)</span></span>", 
          label, prettyNum((round(as.numeric(n) / 5) * 5), big.mark=",")))
  return(result)
}


pvalue <- function(x, ...) {
    # Construct vectors of data y, and groups (strata) g
    y <- unlist(x)
    g <- factor(rep(1:length(x), times=sapply(x, length)))
    if (is.numeric(y)) {
        # For numeric variables, perform a standard 2-sample t-test
        p <- t.test(y ~ g)$p.value
    } else {
        # For categorical variables, perform a chi-squared test of independence
        p <- chisq.test(table(y, g))$p.value
    }
    # Format the p-value, using an HTML entity for the less-than sign.
    # The initial empty string places the output on the line below the variable label.
    c("", sub("<", "&lt;", format.pval(p, digits=3, eps=0.001)))
}


pvalueANOVA <- function(x, ...) {
  # Construct vectors of data y, and groups (strata) g
  y <- unlist(x)
  g <- factor(rep(1:length(x), times=sapply(x, length)))
  
  if (is.numeric(y)) {
    # For numeric variables, perform a standard 2-sample t-test
    ano <- aov(y ~ g)
    p <- summary(ano)[[1]][[5]][1]
    
  } else {
    # For categorical variables, perform a chi-squared test of independence
    p <- chisq.test(table(y, g))$p.value
  }
  # Format the p-value, using an HTML entity for the less-than sign.
  # The initial empty string places the output on the line below the variable label.
  c("", sub("<", "&lt;", format.pval(p, digits=3, eps=0.001)))
}





rndr <- function(x, ...) {
    y <- render.default(x, ...)
    if (is.logical(x)) y[2] else y
}

footnote <- "SDC applied"




# table1(~ SEX + age + age_group + ETHNIC_DESC_HIGH_ONLY + ETHNIC_DESC_DETAIL2 + QUINTILE +
# `Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| treat, 
# data=analysis_cohort_table1,
# render.categorical=my.render.cat,
# footnote=footnote,
# render.strat = my.render.strat,
# render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
# extra.col=list(`P-value`=pvalueANOVA),
# overall=F,
# render=rndr
#        )


```



```{r, tableOneST, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}


# "PRIOR - Beginning", 
#                   "PRIOR - Middle",
#                   "PRIOR - End",
#                   "PANDEMIC - During"


table1(~ age + age_group + SEX + ETHNIC_DESC_HIGH_ONLY + ETHNIC_DESC_DETAIL2 + QUINTILE +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| (surgery_type), 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female","Unknown"))),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalue),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )



```



## Treatment time

Baseline Characteristics stratified by treatment time for SAVR and TAVI

### SAVR

```{r, tableOneTreatment, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + SEX + ETHNIC_DESC_DETAIL2 + ETHNIC_DESC_HIGH_ONLY +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| treat, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%mutate(SEX=factor(SEX,levels=c("Male", 
                  "Female")))%>%filter(surgery_type=="SAVR")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalueANOVA),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )



```


### TAVI

```{r, tableOneTreatmentTavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + SEX + ETHNIC_DESC_DETAIL2 + ETHNIC_DESC_HIGH_ONLY +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| treat, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%mutate(SEX=factor(SEX,levels=c("Male", 
                  "Female")))%>%filter(surgery_type=="TAVI")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalueANOVA),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )


```


## Sex

Baseline Characteristics stratified by sex for SAVR and TAVI

### SAVR

```{r, tableOneSex, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + ETHNIC_DESC_HIGH_ONLY + ETHNIC_DESC_DETAIL2 + QUINTILE +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| SEX, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%filter(surgery_type=="SAVR")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalue),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )


# df1=analysis_cohort_table1 %>%
#   filter(SEX%in%c("Female","Male")) %>% 
#   mutate(SEX=factor(SEX,levels=c("Male", 
#                   "Female"))) %>%
#   select(ETHNIC_DESC_HIGH_ONLY,SEX) %>% ungroup()
# 
# contingency_table <- table(df1$ETHNIC_DESC_HIGH_ONLY, df1$SEX)
# chi_square_result <- chisq.test(contingency_table)

```


### TAVI

```{r, tableOneSexTavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + ETHNIC_DESC_HIGH_ONLY + ETHNIC_DESC_DETAIL2 + QUINTILE +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| SEX, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%filter(surgery_type=="TAVI")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalue),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )


# df1=analysis_cohort_table1 %>%
#   filter(SEX%in%c("Female","Male")) %>% 
#   mutate(SEX=factor(SEX,levels=c("Male", 
#                   "Female"))) %>%
#   select(ETHNIC_DESC_HIGH_ONLY,SEX) %>% ungroup()
# 
# contingency_table <- table(df1$ETHNIC_DESC_HIGH_ONLY, df1$SEX)
# chi_square_result <- chisq.test(contingency_table)

```


## Ethnicity

Baseline Characteristics stratified by ethnicity for SAVR and TAVI

### SAVR

```{r, tableOneEth, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + SEX + QUINTILE +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| ETHNIC_DESC_HIGH_ONLY, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%mutate(SEX=factor(SEX,levels=c("Male", 
                  "Female")))%>%filter(surgery_type=="SAVR")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalueANOVA),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )



```


### TAVI 

```{r, tableOneEthTavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + SEX + QUINTILE +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| ETHNIC_DESC_HIGH_ONLY, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%mutate(SEX=factor(SEX,levels=c("Male", 
                  "Female")))%>%filter(surgery_type=="TAVI")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalueANOVA),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )



```

## Deprivation

Baseline Characteristics stratified by SES quintiles for SAVR and TAVI

### SAVR

```{r, tableOneQuintile, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + SEX + ETHNIC_DESC_DETAIL2 + ETHNIC_DESC_HIGH_ONLY +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| QUINTILE, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%mutate(SEX=factor(SEX,levels=c("Male", 
                  "Female")))%>%filter(surgery_type=="SAVR")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalueANOVA),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )



```


### TAVI

```{r, tableOneQuintileTavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12}

table1(~ age + SEX + ETHNIC_DESC_DETAIL2 + ETHNIC_DESC_HIGH_ONLY +
`Coronary Artery Disease` + Diabetes + Hypertension + `Asthma/COPD` + `Cerebrovascular Disease` + CKD + `Heart Failure` + PAD| QUINTILE, 
data=(analysis_cohort_table1%>%filter(SEX%in%c("Male","Female"))%>%mutate(SEX=factor(SEX,levels=c("Male", 
                  "Female")))%>%filter(surgery_type=="TAVI")),
render.categorical=my.render.cat,
footnote=footnote,
render.strat = my.render.strat,
render.missing = NULL, #render missing values for age as dont know how to do SDC here but also the age groups has the number of missings
extra.col=list(`P-value`=pvalueANOVA),
overall=F,
render=rndr,
render.continuous=my.render.cont
       )



```



```{r, savr, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


analysis_cohort %>% write_rds("tmp/main_cohort_final_first_new4.rds")


```





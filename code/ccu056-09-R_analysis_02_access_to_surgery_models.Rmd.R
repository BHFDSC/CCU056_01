---
title: "CCU056 Analysis"
subtitle: "Analysis 02 - Access/Rates of Surgery"
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

library(emmeans)
library(effects)
library(ggeffects)



install.packages("ggeffects", contriburl = 'file:///mnt/resources/rstudio/miniCranRepo/src/contrib', dependencies = TRUE)

library(ggeffects)

# file_list <- list.files(path = "ggeffects", pattern = "\\.R$", full.names = TRUE)
# 
# # Source each file in the list
# for (file in file_list) {
#   source(file)
# }

#connect
#con <- DBI::dbConnect(odbc:::odbc(), "databricks")

con <- dbConnect(odbc::odbc(), dsn = "databricks", HTTPPath = "sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy", 
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


analysis_cohort = read_rds("tmp/main_cohort_final_first_new4.rds") %>%
  mutate(ETHNIC_DESC_HIGH_ONLY = factor(ETHNIC_DESC_HIGH_ONLY, levels = c("White","Asian or Asian British","Black or Black British","Mixed or Multiple ethnic groups")))


esp = dbGetQuery(con,sprintf(glue(select_all_proj,"esp")))
esp_groups = dbGetQuery(con,sprintf(glue(select_all_proj,"esp_groups")))



```


```{r, parametersPop, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

pop_lsoa_mapped_quintile_age = read_rds("tmp/pop_lsoa_mapped_quintile_age.rds") %>%
  mutate(QUINTILE=as.character(QUINTILE))
pop_lsoa_mapped_quintile_sex_age = read_rds("tmp/pop_lsoa_mapped_quintile_sex_age.rds") %>%
  mutate(QUINTILE=as.character(QUINTILE))
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


# Access to Surgery

* All counts have been averaged by the number of years in the treatment period. E.g. In 2000-2004 the average count for this period is the years 2000-2004 averaged by the 5 years in this period whilst in 2020-2023 the average count for this period is the years 2020-2023 averaged by the 4 years in this period. Rates are expressed per year.

```{r, ats, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

# Prepare dataset
access_to_survery_df = analysis_cohort %>%
  mutate(treatment_time = case_when(
    OPERATION_YEAR >=2000 & OPERATION_YEAR <=2004 ~ "2000-2004",
    OPERATION_YEAR >=2005 & OPERATION_YEAR <=2009 ~ "2005-2009", 
    OPERATION_YEAR >=2010 & OPERATION_YEAR <=2014 ~ "2010-2014", 
    OPERATION_YEAR >=2015 & OPERATION_YEAR <=2019 ~ "2015-2019",
    OPERATION_YEAR >=2020 & OPERATION_YEAR <=2023 ~ "2020-2023",
    TRUE ~ NA_character_)) %>%
  select(surgery_type,Sex=SEX,age_group,
         treatment_time,
         #treatment_time=treat,
         Deprivation=QUINTILE,Ethnicity=ETHNIC_DESC_HIGH_ONLY) %>%
  ungroup() %>%
  filter(age_group!="Unknown") %>%
  mutate(surgery_type=ifelse(surgery_type=="savr","SAVR","TAVI")) %>%
  mutate(surgery_type=factor(surgery_type, levels=c("SAVR","TAVI"))) %>%
  # mutate(treatment_time = case_when(
  #   treatment_time == "PRIOR - Beginning" ~ "2000-2004",
  #   treatment_time == "PRIOR - Middle" ~ "2005-2014", 
  #   treatment_time == "PRIOR - End" ~ "2015-2019",
  #   treatment_time == "PANDEMIC - During" ~ "2020-2023",
  #   TRUE ~ NA_character_)) %>%
  mutate(treatment_time=factor(treatment_time,
                               levels=c("2000-2004",
                                        "2005-2009",
                                        "2010-2014",
                                        "2015-2019",
                                        "2020-2023"))) %>%
  

  mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups")))




```


```{r, eda, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE, fig.width=12,fig.height=10}


# ## EDA - Age Distribution

access_to_survery_df %>%
  group_by(age_group,surgery_type) %>% count() %>%
  ggplot(aes(x=age_group, y=n, fill=surgery_type)) +
  geom_bar(stat="identity") +
  facet_wrap(surgery_type ~ ., scales='free', ncol= 1) +
  scale_fill_manual(name = "", values = c("SAVR" = savr_colour,
                                           "TAVI" = tavi_colour)) + 
  theme(legend.position = "none") +
  labs(title='Age Distribution',x="")
  


```


## Ethnicity

Unknown Ethnicity group removed.

Model: Poisson GLM with log population offset.

```{r, eth_working_archive, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

 
eth_access_to_survery_df = 
access_to_survery_df %>%
  filter(Ethnicity!="Unknown") %>%
  group_by(surgery_type,Ethnicity,age_group,treatment_time) %>% count() %>%
  left_join((pop_ethnicity_mapped_ethnicity_high_age),
            by = join_by("Ethnicity"=="project_high", age_group)) %>%
  rename(population=pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(population=log(population)) %>%
  #now work out count average in the treatment time years
  mutate(treatment_time_length = ifelse(treatment_time=="2020-2023",4,5)) %>%
  mutate(n=n/treatment_time_length)


# Models - Fit for SAVR and TAVI separately
# Modelling Ethnicity and Treatment time with their interaction
# Controlling for Age Group
# Offsetting the count by the population
eth_models_list = list()

eth_models_list[[1]] = glm(n ~ Ethnicity + treatment_time + Ethnicity * treatment_time + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")),
                           family = "poisson")
eth_models_list[[2]] = glm(n ~ Ethnicity + treatment_time + Ethnicity * treatment_time + age_group + offset(log(population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "TAVI")),
                           family = "poisson")


model = eth_models_list[[1]]
ggemmeans(eth_models_list[[1]], c('Ethnicity','treatment_time'))


hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr")

hypothesis_test(model, c("Ethnicity", "treatment_time"), scale="irr")
hypothesis_test(model, c("Ethnicity", "treatment_time"), scale="irr", test=NULL)
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test=NULL)
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b2=b3")


# Asian vs Black difference 2000 vs 2005
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2005-2009]"), scale="irr", test=NULL)

hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2005-2009]"), scale="irr", test="(b3-(b5))=(b4-(b6))")

# Asian vs All in 2000
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test=NULL)

hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="(b2)=(b1+b3+b4+b5)")


# Asian vs All Difference in 2000 vs 2005
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2005-2009]"), scale="irr", test=NULL)

hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2005-2009]"), scale="irr", test="(b3-(b1+b5+b7+b9))=(b4-(b2+b6+b8+b10)), b1=b2")


hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test=NULL)
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), test=NULL)



ethnicity_test_labels = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Ethnicity) %>%
  mutate(test=paste0("b",row_number()))


# Ethnicity - each level vs Rest in 2000-2004
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b1=(b2+b3+b4+b5)") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b2=(b1+b3+b4+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b3=(b1+b2+b4+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b4=(b1+b2+b3+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b5=(b1+b2+b3+b4)") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " vs Rest")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 4), scientific = TRUE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


# Ethnicity - each level vs Rest in 2015-2019
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b1=(b2+b3+b4+b5)") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b2=(b1+b3+b4+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b3=(b1+b2+b4+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b4=(b1+b2+b3+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b5=(b1+b2+b3+b4)") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " vs Rest")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 4), scientific = TRUE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


# Ethnicity - each level vs Rest in 2020-2023
hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b1=(b2+b3+b4+b5)") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b2=(b1+b3+b4+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b3=(b1+b2+b4+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b4=(b1+b2+b3+b5)") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b5=(b1+b2+b3+b4)") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " vs Rest")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 4), scientific = TRUE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", p_adjust = "tukey")
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr") %>%
  as.data.frame() %>%
  mutate(Hypothesis=str_replace(Ethnicity,"-"," vs ")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 4), scientific = TRUE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)




(subset(eth_access_to_survery_df, surgery_type == "SAVR"))  %>%
  filter(treatment_time=="2000-2004") %>% ungroup() %>% filter(Ethnicity=="White") %>%
  mutate(population=exp(population))  %>%
  mutate(rate=n/population) %>%
  #arrange(rate) %>%
  mutate(count=rate*exp(11.6))


(subset(eth_access_to_survery_df, surgery_type == "SAVR"))  %>%
  filter(treatment_time=="2000-2004") %>% ungroup() %>% filter(Ethnicity=="White") %>%
  mutate(population=exp(population))  %>%
  mutate(rate=n/population) %>%
  #arrange(rate) %>%
  mutate(count=rate*exp(11.6)) %>%
  summarise(n=mean(population))

```


```{r, eth_working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}
 

eth_access_to_survery_df = 
access_to_survery_df %>%
  filter(Ethnicity!="Unknown") %>%
  group_by(surgery_type,Ethnicity,age_group,treatment_time) %>% count() %>%
  left_join((pop_ethnicity_mapped_ethnicity_high_age),
            by = join_by("Ethnicity"=="project_high", age_group)) %>%
  rename(population=pop_ethnicity_var_ethnicity_high_age) %>%
  mutate(population=log(population)) %>%
  #now work out count average in the treatment time years
  mutate(treatment_time_length = ifelse(treatment_time=="2020-2023",4,5)) %>%
  mutate(n=n/treatment_time_length)



# Models - Fit for SAVR and TAVI separately
# Modelling Ethnicity and Treatment time with their interaction
# Controlling for Age Group
# Offsetting the count by the population
eth_models_list = list()

eth_models_list[[1]] = glm(n ~ Ethnicity + treatment_time + Ethnicity * treatment_time + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")),
                           family = "poisson")

eth_models_list[[2]] = glm(n ~ Ethnicity + treatment_time + Ethnicity * treatment_time + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "TAVI")),
                           family = "poisson")


# Calculate marginal means, interaction effects, and contrasts for each surgery_type
eth_results_list <- lapply(eth_models_list, function(model) {
  
  # Calculate marginal means - the average response (here, rates n) for each combination of the predictor variables (Ethnicity and treatment_time), while averaging over the other variables (age_group).
  
  #eth_emm <- emmeans(model, ~ Ethnicity * treatment_time,type = "response")
  eth_emm <- ggemmeans(model, c('Ethnicity','treatment_time'))

  # Extract interaction effects - the estimated marginal means for Ethnicity within each level of treatment_time. E.g. How the interaction effect and how the rates vary between Ethnicity groups (e,g, White Black Asian etc) for each treatment_time period.
  # eth_inter_emm <- emmeans(model, ~ Ethnicity | treatment_time,type = "response")

  # Create contrasts - specific pairwise comparisons for the interaction effect, considering the control for age -  indicating whether there are significant differences in rates between each ethnicity group for each time period, while controlling for age.
  #eth_contrasts <- contrast(eth_inter_emm, method = "pairwise")
  eth_contrasts <- hypothesis_test(model, c("Ethnicity", "treatment_time"), type="irr")
  
  

  # Store results in a list
  results <- list(emm = eth_emm, contrasts = eth_contrasts)
  return(results)
})

# Results for each surgery type
eth_results_savr <- eth_results_list[[1]]
eth_results_tavi <- eth_results_list[[2]]


```



### SAVR

#### Predicted rates per year per 100,000 people - held at mean age

Held at age group: 70-74 (mean age of SAVR is 70)

```{r, eth_working_savr_rates_population, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# running this to see what the population is adjusted for - for working in per 100,000
ggemmeans(eth_models_list[[1]], c('Ethnicity','treatment_time', 'age_group [70-74]'))

# Adjusted for population 11.94
# exp(11.94) = 153276.7
```

```{r, eth_working_savr_rates_per, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


eth_savr_rates_per_raw = 
ggemmeans(eth_models_list[[1]], c('Ethnicity','treatment_time', 'age_group [70-74]')) %>% 
  as.data.frame() %>%
  # per 100000
  mutate(predicted=predicted*exp(11.94)/100000) %>%
  mutate(conf.low=conf.low*exp(11.94)/100000) %>%
  mutate(conf.high=conf.high*exp(11.94)/100000) %>%
  rename(Group=group,Ethnicity=x)
  
eth_savr_rates_per = 
  eth_savr_rates_per_raw %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Predicted = format(round(predicted, digits = 2), nsmall = 2)) %>%
  select(Group,Ethnicity,Predicted,`95% CI`) %>%
  arrange(Group, factor(Ethnicity, levels = c(
"White",
"Asian or Asian British",
"Black or Black British",
"Mixed or Multiple ethnic groups",
"Other ethnic groups")))
  


kableExtra::kbl(
  eth_savr_rates_per %>% select(-Group),
    booktabs = T, 
    caption=("Ethnicity SAVR Rates per 100,000 per year held at mean SAVR age group"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 4) %>%
    group_rows("2005 - 2009", 5, 8) %>%
    group_rows("2010 - 2014", 9, 12) %>%
    group_rows("2015 - 2019", 13, 16) %>%
    group_rows("2020 - 2023", 17, 20)


```


```{r, eth_results_plot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}

# Plot
eth_plot_df <- eth_savr_rates_per_raw %>% mutate(surgery_type="SAVR")

eth_plot_df %>%
  rename(n=predicted,treatment_time=Group) %>%
  ggplot(aes(x=treatment_time, y=n, group=Ethnicity, color=Ethnicity)) + theme_bw()+
   facet_wrap(surgery_type ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at average age) over Time',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100"))


```




```{r, eth_results_plot2, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}


eth_plot_df %>%
  filter(Group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=Group) %>%
  mutate(dummy="1") %>%
  ggplot(aes(x=dummy, y=n, group=Ethnicity, color=Ethnicity)) + theme_bw()+
   facet_wrap(treatment_time ~ ., nrow=1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at mean age) for pre, preceding and during pandemic periods',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100",
                                           "Other ethnic groups" = "#7346CB")) +
  theme(axis.text.x = element_blank(),
        panel.grid.major = element_blank(),
    panel.grid.minor = element_blank())



```

#### Predicted rates per year per 100,000 people - all age groups

```{r, eth_results_plot3, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

ggemmeans(eth_models_list[[1]], c('Ethnicity','treatment_time', 'age_group')) %>%
  as.data.frame() %>%
  mutate(predicted=predicted*exp(11.94)/100000) %>%
  mutate(conf.low=conf.low*exp(11.94)/100000) %>%
  mutate(conf.high=conf.high*exp(11.94)/100000) %>%
  filter(group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=group,age=facet,Ethnicity=x) %>%
  ggplot(aes(x=age, y=n, group=Ethnicity, color=Ethnicity)) + theme_bw()+
   facet_wrap(treatment_time ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100",
                                           "Other ethnic groups" = "#7346CB")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
  

```


```{r, eth_working_savr_all_pairwise, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

replace_second_dash <- function(x) {
  first_dash_index <- which(strsplit(x, "")[[1]] == "-")[1]  # Find the index of the first dash
  second_dash_index <- which(strsplit(x, "")[[1]] == "-")[2] # Find the index of the second dash
  
  if (!is.na(second_dash_index)) {
    x <- paste0(substr(x, 1, second_dash_index - 1), " - ", substr(x, second_dash_index + 1, nchar(x)))
  }
  
  return(x)
}



eth_savr_all_pairwise = 
eth_results_savr[[2]] %>% as.data.frame() %>%
  mutate(Hypothesis=str_replace(Ethnicity,"-"," - ")) %>%

  mutate(treatment_time = sapply(treatment_time, replace_second_dash)) %>%


  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 4), scientific = TRUE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p) %>%

  arrange(treatment_time,
          factor(Hypothesis, levels = c(
"White - White",
"Asian or Asian British - Asian or Asian British",
"Black or Black British - Black or Black British",
"Mixed or Multiple ethnic groups - Mixed or Multiple ethnic groups",
"Other ethnic groups - Other ethnic groups",
"White - Asian or Asian British",
"White - Black or Black British",
"White - Mixed or Multiple ethnic groups",
"White - Other ethnic groups",
"Asian or Asian British - Black or Black British",
"Asian or Asian British - Mixed or Multiple ethnic groups",
"Asian or Asian British - Other ethnic groups",
"Black or Black British - Mixed or Multiple ethnic groups",
"Black or Black British - Other ethnic groups",
"Mixed or Multiple ethnic groups - Other ethnic groups"))
)


kableExtra::kbl(
  eth_savr_all_pairwise,
    booktabs = T, 
    caption=("Ethnicity SAVR Pairwise Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    )





```

#### Contrasts

Setting White as the reference group

```{r, eth_working_savr, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = eth_models_list[[1]]

ethnicity_test_labels = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Ethnicity) %>%
  mutate(test=paste0("b",row_number()))


# Ethnicity - each level vs Rest in 2000-2004 then join on other time periods
eth_savr_custom_t1 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


eth_savr_custom_t2 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)



eth_savr_custom_t3 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>% 
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


eth_savr_custom = 
eth_savr_custom_t1 %>% rbind(eth_savr_custom_t2) %>% rbind(eth_savr_custom_t3)


kableExtra::kbl(
  eth_savr_custom %>% select(-treatment_time),
    booktabs = T, 
    caption=("Ethnicity SAVR Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 3) %>%
    group_rows("2015 - 2019", 4, 6) %>%
    group_rows("2020 - 2023", 7, 9)



```


##### Differences

Now considering differences:

eg Asian-White difference in 2000-2004 compared to 2015-2019
Asian-White difference in 2015-2019 compared to 2020-2023


```{r, eth_working_savr_diss, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = eth_models_list[[1]]

ethnicity_test_labels_t1 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Ethnicity) %>%
  mutate(test=paste0("b",row_number()))

ethnicity_test_labels_t2 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Ethnicity) %>%
  mutate(test=paste0("b",row_number()))



eth_savr_custom_contrasts_t1 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b5-(b1))=(b6-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b7-(b1))=(b8-(b2))") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(ethnicity_test_labels_t1,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White Difference")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004 - 2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


eth_savr_custom_contrasts_t2 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b5-(b1))=(b6-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b7-(b1))=(b8-(b2))") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(ethnicity_test_labels_t2,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White Difference")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019 - 2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


eth_savr_custom_contrasts = 
eth_savr_custom_contrasts_t1 %>% rbind(eth_savr_custom_contrasts_t2)


kableExtra::kbl(
  (eth_savr_custom_contrasts%>%select(-treatment_time)),
    booktabs = T, 
    caption=("Ethnicity SAVR Differences Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000-2004 - 2015-2019", 1, 3) %>%
    group_rows("2015-2019 - 2020-2023", 4, 6)

```




#### Relative Risks

##### Unadjusted

```{r, eth_working_savr_irr_unadj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

eth_access_to_survery_df = eth_access_to_survery_df %>% mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups")))
  
eth_model_2000_unadj = glm(n ~ Ethnicity +  offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2000-2004")),
                           family = "poisson")

eth_irr_2000 = exp(coef(eth_model_2000_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2000_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))


eth_model_2015_unadj = glm(n ~ Ethnicity  + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

eth_irr_2015 = exp(coef(eth_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))



eth_model_2020_unadj = glm(n ~ Ethnicity  + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

eth_irr_2020 = exp(coef(eth_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))

eth_irr_unadj = eth_irr_2000 %>% mutate(treatment_time="2000-2004") %>%
  rbind(eth_irr_2015 %>% mutate(treatment_time="2015-2019")) %>%
  rbind(eth_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  eth_irr_unadj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Ethnicity SAVR Relative Risk - Unadjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 3) %>%
    group_rows("2015 - 2019", 4, 6) %>%
    group_rows("2020 - 2023", 7, 9)

```


##### Adjusted

```{r, eth_working_savr_irr_adj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

eth_access_to_survery_df = eth_access_to_survery_df %>% mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups")))
  
eth_model_2000_adj = glm(n ~ Ethnicity + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2000-2004")),
                           family = "poisson")

eth_irr_2000 = exp(coef(eth_model_2000_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2000_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))


eth_model_2015_adj = glm(n ~ Ethnicity + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

eth_irr_2015 = exp(coef(eth_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))



eth_model_2020_adj = glm(n ~ Ethnicity + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

eth_irr_2020 = exp(coef(eth_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))

eth_irr_adj = eth_irr_2000 %>% mutate(treatment_time="2000-2004") %>%
  rbind(eth_irr_2015 %>% mutate(treatment_time="2015-2019")) %>%
  rbind(eth_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  eth_irr_adj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Ethnicity SAVR Relative Risk - Adjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 3) %>%
    group_rows("2015 - 2019", 4, 6) %>%
    group_rows("2020 - 2023", 7, 9)

```



```{r, eth_working_savr_irr_old, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

eth_access_to_survery_df = eth_access_to_survery_df %>% mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups")))
  
eth_models_list[[1]] = glm(n ~ Ethnicity + treatment_time + Ethnicity * treatment_time + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "SAVR")),
                           family = "poisson")

eth_irr = exp(coef(eth_models_list[[1]])) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_models_list[[1]])) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(str_detect(parameter,":treatment_time")) %>%
  filter(str_detect(parameter,"2015-2019|2020-2023")) %>%
  separate(parameter,sep=":treatment_time",into=c("Ethnicity","treatment_time")) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))



kableExtra::kbl(
  eth_irr %>% select(-treatment_time),
    booktabs = T, 
    caption=("Ethnicity SAVR Relative Risk"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2015 - 2019", 1, 3) %>%
    group_rows("2020 - 2023", 4, 6)

```




### TAVI

#### Predicted rates per year per 100,000 people - held at mean age

Held at age group: 80-84 (mean age of TAVI is 80)

```{r, eth_working_tavi_rates_population, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

eth_models_list[[2]] = glm(n ~ Ethnicity + treatment_time + Ethnicity * treatment_time + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "TAVI")),
                           family = "poisson")

# running this to see what the population is adjusted for - for working in per 100,000
ggemmeans(eth_models_list[[2]], c('Ethnicity','treatment_time', 'age_group [80-84]'))

# Adjusted for population 11.99

```

```{r, eth_working_tavi_rates_per, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


eth_tavi_rates_per_raw = 
ggemmeans(eth_models_list[[2]], c('Ethnicity','treatment_time', 'age_group [80-84]')) %>% 
  as.data.frame() %>%
  # per 100000
  mutate(predicted=predicted*exp(11.99)/100000) %>%
  mutate(conf.low=conf.low*exp(11.99)/100000) %>%
  mutate(conf.high=conf.high*exp(11.99)/100000) %>%
  rename(Group=group,Ethnicity=x)
  
eth_tavi_rates_per = 
  eth_tavi_rates_per_raw %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Predicted = format(round(predicted, digits = 2), nsmall = 2)) %>%
  select(Group,Ethnicity,Predicted,`95% CI`) %>%
  arrange(Group, factor(Ethnicity, levels = c(
"White",
"Asian or Asian British",
"Black or Black British",
"Mixed or Multiple ethnic groups",
"Other ethnic groups")))
  


kableExtra::kbl(
  eth_tavi_rates_per %>% select(-Group),
    booktabs = T, 
    caption=("Ethnicity TAVI Rates per 100,000 per year held at mean TAVI age group"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2005 - 2009", 1, 4) %>%
    group_rows("2010 - 2014", 5, 8) %>%
    group_rows("2015 - 2019", 9, 12) %>%
    group_rows("2020 - 2023", 13, 16)


```


```{r, eth_results_plot_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}

# Plot
eth_plot_df_tavi <- eth_tavi_rates_per_raw %>% mutate(surgery_type="TAVI")

eth_plot_df_tavi %>%
  rename(n=predicted,treatment_time=Group) %>%
  ggplot(aes(x=treatment_time, y=n, group=Ethnicity, color=Ethnicity)) + theme_bw()+
   facet_wrap(surgery_type ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at average age) over Time',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100"))


```




```{r, eth_results_plot2_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}


eth_plot_df_tavi %>%
  filter(Group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=Group) %>%
  mutate(dummy="1") %>%
  ggplot(aes(x=dummy, y=n, group=Ethnicity, color=Ethnicity)) + theme_bw()+
   facet_wrap(treatment_time ~ ., nrow=1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at mean age) for pre, preceding and during pandemic periods',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100",
                                           "Other ethnic groups" = "#7346CB")) +
  theme(axis.text.x = element_blank(),
        panel.grid.major = element_blank(),
    panel.grid.minor = element_blank())



```

#### Predicted rates per year per 100,000 people - all age groups

```{r, eth_results_plot3_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

ggemmeans(eth_models_list[[2]], c('Ethnicity','treatment_time', 'age_group')) %>%
  as.data.frame() %>%
  mutate(predicted=predicted*exp(11.99)/100000) %>%
  mutate(conf.low=conf.low*exp(11.99)/100000) %>%
  mutate(conf.high=conf.high*exp(11.99)/100000) %>%
  filter(group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=group,age=facet,Ethnicity=x) %>%
  ggplot(aes(x=age, y=n, group=Ethnicity, color=Ethnicity)) + theme_bw()+
   facet_wrap(treatment_time ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("White" = "#FFCD16",
                                           "Asian or Asian British" = "#FF042E",
                                           "Black or Black British" = "#3CB08C",
                                           "Mixed or Multiple ethnic groups" = "#FF5100",
                                           "Other ethnic groups" = "#7346CB")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
  

```


#### Contrasts

Setting White as the reference group

```{r, eth_working_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = eth_models_list[[2]]

ethnicity_test_labels = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Ethnicity) %>%
  mutate(test=paste0("b",row_number()))



eth_tavi_custom_t2 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)



eth_tavi_custom_t3 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2020-2023]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>% 
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(ethnicity_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


eth_tavi_custom = 
eth_tavi_custom_t2 %>% rbind(eth_tavi_custom_t3) 


kableExtra::kbl(
  eth_tavi_custom %>% select(-treatment_time),
    booktabs = T, 
    caption=("Ethnicity TAVI Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    
    group_rows("2015 - 2019", 1, 3) %>%
    group_rows("2020 - 2023", 4, 6)



```


##### Differences


```{r, eth_working_tavi_diss, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = eth_models_list[[2]]


ethnicity_test_labels_t2 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Ethnicity) %>%
  mutate(test=paste0("b",row_number()))



eth_tavi_custom_contrasts_t2 = 
hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b5-(b1))=(b6-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Ethnicity", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b7-(b1))=(b8-(b2))") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(ethnicity_test_labels_t2,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Ethnicity, " - White Difference")) %>%
  select(-Ethnicity) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019 - 2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


eth_tavi_custom_contrasts = 
eth_tavi_custom_contrasts_t2


kableExtra::kbl(
  (eth_tavi_custom_contrasts%>%select(-treatment_time)),
    booktabs = T, 
    caption=("Ethnicity TAVI Differences Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015-2019 - 2020-2023", 1, 3)

```




#### Relative Risks

##### Unadjusted

```{r, eth_working_tavi_irr_unadj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

eth_access_to_survery_df = eth_access_to_survery_df %>% mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups")))
  


eth_model_2015_unadj = glm(n ~ Ethnicity  + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

eth_irr_2015 = exp(coef(eth_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))



eth_model_2020_unadj = glm(n ~ Ethnicity  + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

eth_irr_2020 = exp(coef(eth_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))

eth_irr_unadj = eth_irr_2015 %>% mutate(treatment_time="2015-2019") %>%
  
  rbind(eth_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  eth_irr_unadj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Ethnicity TAVI Relative Risk - Unadjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015 - 2019", 1, 3) %>%
    group_rows("2020 - 2023", 4, 6)

```


##### Adjusted

```{r, eth_working_tavi_irr_adj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

eth_access_to_survery_df = eth_access_to_survery_df %>% mutate(Ethnicity=factor(Ethnicity,
                                           levels=c("White",
                                                    "Asian or Asian British",
                                                    "Black or Black British",
                                                    "Mixed or Multiple ethnic groups")))

eth_model_2015_adj = glm(n ~ Ethnicity + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

eth_irr_2015 = exp(coef(eth_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))



eth_model_2020_adj = glm(n ~ Ethnicity + age_group + offset((population)),
                           data = (subset(eth_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

eth_irr_2020 = exp(coef(eth_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(eth_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Ethnicity=parameter) %>%
  mutate(Ethnicity=str_replace(Ethnicity,"Ethnicity",""))

eth_irr_adj = eth_irr_2015 %>% mutate(treatment_time="2015-2019") %>%

  rbind(eth_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  eth_irr_adj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Ethnicity TAVI Relative Risk - Adjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015 - 2019", 1, 3) %>%
    group_rows("2020 - 2023", 4, 6)

```




## Deprivation


Unknown IMD Quintile group removed.

Model: Poisson GLM with log population offset.


```{r, dep_working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}
 

dep_access_to_survery_df = 
access_to_survery_df %>%
  filter(Deprivation!="Unknown") %>%
  group_by(surgery_type,Deprivation,age_group,treatment_time) %>% count() %>%
  left_join((pop_lsoa_mapped_quintile_age),
            by = join_by("Deprivation"=="QUINTILE", age_group)) %>%
  rename(population=pop_lsoa_var_quintile_age) %>%
  mutate(population=log(population)) %>%
  #now work out count average in the treatment time years
  mutate(treatment_time_length = ifelse(treatment_time=="2020-2023",4,5)) %>%
  mutate(n=n/treatment_time_length)


# Models - Fit for SAVR and TAVI separately
# Modelling Deprivation and Treatment time with their interaction
# Controlling for Age Group
# Offsetting the count by the population
dep_models_list = list()

dep_models_list[[1]] = glm(n ~ Deprivation + treatment_time + Deprivation * treatment_time + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "SAVR")),
                           family = "poisson")

dep_models_list[[2]] = glm(n ~ Deprivation + treatment_time + Deprivation * treatment_time + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "TAVI")),
                           family = "poisson")


# Calculate marginal means, interaction effects, and contrasts for each surgery_type
dep_results_list <- lapply(dep_models_list, function(model) {
  
  # Calculate marginal means - the average response (here, rates n) for each combination of the predictor variables (Deprivation and treatment_time), while averaging over the other variables (age_group).
  
  #dep_emm <- emmeans(model, ~ Deprivation * treatment_time,type = "response")
  dep_emm <- ggemmeans(model, c('Deprivation','treatment_time'))

  # Extract interaction effects - the estimated marginal means for Deprivation within each level of treatment_time. E.g. How the interaction effect and how the rates vary between Deprivation groups (e,g, White Black Asian etc) for each treatment_time period.
  # dep_inter_emm <- emmeans(model, ~ Deprivation | treatment_time,type = "response")

  # Create contrasts - specific pairwise comparisons for the interaction effect, considering the control for age -  indicating whether there are significant differences in rates between each Deprivation group for each time period, while controlling for age.
  #dep_contrasts <- contrast(dep_inter_emm, method = "pairwise")
  dep_contrasts <- hypothesis_test(model, c("Deprivation", "treatment_time"), type="irr")
  
  

  # Store results in a list
  results <- list(emm = dep_emm, contrasts = dep_contrasts)
  return(results)
})

# Results for each surgery type
dep_results_savr <- dep_results_list[[1]]
dep_results_tavi <- dep_results_list[[2]]


```



### SAVR

#### Predicted rates per year per 100,000 people - held at mean age

Held at age group: 70-74 (mean age of SAVR is 70)

```{r, dep_working_savr_rates_population, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# running this to see what the population is adjusted for - for working in per 100,000
ggemmeans(dep_models_list[[1]], c('Deprivation','treatment_time', 'age_group [70-74]'))

# Adjusted for population 13.30

```

```{r, dep_working_savr_rates_per, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


dep_savr_rates_per_raw = 
ggemmeans(dep_models_list[[1]], c('Deprivation','treatment_time', 'age_group [70-74]')) %>% 
  as.data.frame() %>%
  # per 100000
  mutate(predicted=predicted*exp(13.30)/100000) %>%
  mutate(conf.low=conf.low*exp(13.30)/100000) %>%
  mutate(conf.high=conf.high*exp(13.30)/100000) %>%
  rename(Group=group,Deprivation=x)
  
dep_savr_rates_per = 
  dep_savr_rates_per_raw %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Predicted = format(round(predicted, digits = 2), nsmall = 2)) %>%
  select(Group,Deprivation,Predicted,`95% CI`) %>%
  arrange(Group, factor(Deprivation, levels = c(
"1","2","3","4","5")))
  


kableExtra::kbl(
  dep_savr_rates_per %>% select(-Group),
    booktabs = T, 
    caption=("Deprivation SAVR Rates per 100,000 per year held at mean SAVR age group"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 5) %>%
    group_rows("2005 - 2009", 6, 10) %>%
    group_rows("2010 - 2014", 11, 15) %>%
    group_rows("2015 - 2019", 16, 20) %>%
    group_rows("2020 - 2023", 21, 25)


```


```{r, dep_results_plot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}

# Plot
dep_plot_df <- dep_savr_rates_per_raw %>% mutate(surgery_type="SAVR")

dep_plot_df %>%
  rename(n=predicted,treatment_time=Group) %>%
  ggplot(aes(x=treatment_time, y=n, group=Deprivation, color=Deprivation)) + theme_bw()+
   facet_wrap(surgery_type ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at average age) over Time',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793"))


```




```{r, dep_results_plot2, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}


dep_plot_df %>%
  filter(Group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=Group) %>%
  mutate(dummy="1") %>%
  ggplot(aes(x=dummy, y=n, group=Deprivation, color=Deprivation)) + theme_bw()+
   facet_wrap(treatment_time ~ ., nrow=1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at mean age) for pre, preceding and during pandemic periods',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793")) +
  theme(axis.text.x = element_blank(),
        panel.grid.major = element_blank(),
    panel.grid.minor = element_blank())



```

#### Predicted rates per year per 100,000 people - all age groups

```{r, dep_results_plot3, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

ggemmeans(dep_models_list[[1]], c('Deprivation','treatment_time', 'age_group')) %>%
  as.data.frame() %>%
  mutate(predicted=predicted*exp(13.30)/100000) %>%
  mutate(conf.low=conf.low*exp(13.30)/100000) %>%
  mutate(conf.high=conf.high*exp(13.30)/100000) %>%
  filter(group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=group,age=facet,Deprivation=x) %>%
  ggplot(aes(x=age, y=n, group=Deprivation, color=Deprivation)) + theme_bw()+
   facet_wrap(treatment_time ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
  

```



#### Contrasts

Setting White as the reference group

```{r, dep_working_savr, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = dep_models_list[[1]]

Deprivation_test_labels = 
hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Deprivation) %>%
  mutate(test=paste0("b",row_number()))


# Deprivation - each level vs Rest in 2000-2004 then join on other time periods
dep_savr_custom_t1 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004]"), scale="irr", test="b5=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Deprivation_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


dep_savr_custom_t2 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b5=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Deprivation_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)



dep_savr_custom_t3 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b5=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Deprivation_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


dep_savr_custom = 
dep_savr_custom_t1 %>% rbind(dep_savr_custom_t2) %>% rbind(dep_savr_custom_t3)


kableExtra::kbl(
  dep_savr_custom %>% select(-treatment_time),
    booktabs = T, 
    caption=("Deprivation SAVR Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 4) %>%
    group_rows("2015 - 2019", 5, 8) %>%
    group_rows("2020 - 2023", 9, 12)



```


##### Differences

Now considering differences:

eg Asian-White difference in 2000-2004 compared to 2015-2019
Asian-White difference in 2015-2019 compared to 2020-2023


```{r, dep_working_savr_diss, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = dep_models_list[[1]]

Deprivation_test_labels_t1 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Deprivation) %>%
  mutate(test=paste0("b",row_number()))

Deprivation_test_labels_t2 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Deprivation) %>%
  mutate(test=paste0("b",row_number()))



dep_savr_custom_contrasts_t1 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b5-(b1))=(b6-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b7-(b1))=(b8-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b9-(b1))=(b10-(b2))") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(Deprivation_test_labels_t1,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1 Difference")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004 - 2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


dep_savr_custom_contrasts_t2 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b5-(b1))=(b6-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b7-(b1))=(b8-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b9-(b1))=(b10-(b2))") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(Deprivation_test_labels_t2,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1 Difference")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019 - 2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


dep_savr_custom_contrasts = 
dep_savr_custom_contrasts_t1 %>% rbind(dep_savr_custom_contrasts_t2)


kableExtra::kbl(
  (dep_savr_custom_contrasts%>%select(-treatment_time)),
    booktabs = T, 
    caption=("Deprivation SAVR Differences Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000-2004 - 2015-2019", 1, 4) %>%
    group_rows("2015-2019 - 2020-2023", 5, 8)


```




#### Relative Risks

##### Unadjusted

```{r, dep_working_savr_irr_unadj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

dep_access_to_survery_df = dep_access_to_survery_df %>% mutate(Deprivation=factor(Deprivation,
                                           levels=c("1","2","3","4","5")))
  
dep_model_2000_unadj = glm(n ~ Deprivation +  offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2000-2004")),
                           family = "poisson")

dep_irr_2000 = exp(coef(dep_model_2000_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2000_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))


dep_model_2015_unadj = glm(n ~ Deprivation  + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

dep_irr_2015 = exp(coef(dep_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))



dep_model_2020_unadj = glm(n ~ Deprivation  + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

dep_irr_2020 = exp(coef(dep_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))

dep_irr_unadj = dep_irr_2000 %>% mutate(treatment_time="2000-2004") %>%
  rbind(dep_irr_2015 %>% mutate(treatment_time="2015-2019")) %>%
  rbind(dep_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  dep_irr_unadj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Deprivation SAVR Relative Risk - Unadjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 4) %>%
    group_rows("2015 - 2019", 5, 8) %>%
    group_rows("2020 - 2023", 9, 12)

```


##### Adjusted

```{r, dep_working_savr_irr_adj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

dep_access_to_survery_df = dep_access_to_survery_df %>% mutate(Deprivation=factor(Deprivation,
                                           levels=c("1","2","3","4","5")))
  
dep_model_2000_adj = glm(n ~ Deprivation + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2000-2004")),
                           family = "poisson")

dep_irr_2000 = exp(coef(dep_model_2000_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2000_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))


dep_model_2015_adj = glm(n ~ Deprivation + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

dep_irr_2015 = exp(coef(dep_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))



dep_model_2020_adj = glm(n ~ Deprivation + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

dep_irr_2020 = exp(coef(dep_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))

dep_irr_adj = dep_irr_2000 %>% mutate(treatment_time="2000-2004") %>%
  rbind(dep_irr_2015 %>% mutate(treatment_time="2015-2019")) %>%
  rbind(dep_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  dep_irr_adj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Deprivation SAVR Relative Risk - Adjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 4) %>%
    group_rows("2015 - 2019", 5, 8) %>%
    group_rows("2020 - 2023", 9, 12)

```






### TAVI

#### Predicted rates per year per 100,000 people - held at mean age

Held at age group: 80-84 (mean age of TAVI is 80)

```{r, dep_working_tavi_rates_population, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

dep_models_list[[2]] = glm(n ~ Deprivation + treatment_time + Deprivation * treatment_time + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "TAVI")),
                           family = "poisson")

# running this to see what the population is adjusted for - for working in per 100,000
ggemmeans(dep_models_list[[2]], c('Deprivation','treatment_time', 'age_group [80-84]'))

# Adjusted for population 11.99

```

```{r, dep_working_tavi_rates_per, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


dep_tavi_rates_per_raw = 
ggemmeans(dep_models_list[[2]], c('Deprivation','treatment_time', 'age_group [80-84]')) %>% 
  as.data.frame() %>%
  # per 100000
  mutate(predicted=predicted*exp(13.17)/100000) %>%
  mutate(conf.low=conf.low*exp(13.17)/100000) %>%
  mutate(conf.high=conf.high*exp(13.17)/100000) %>%
  rename(Group=group,Deprivation=x)
  
dep_tavi_rates_per = 
  dep_tavi_rates_per_raw %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Predicted = format(round(predicted, digits = 2), nsmall = 2)) %>%
  select(Group,Deprivation,Predicted,`95% CI`) %>%
  arrange(Group, factor(Deprivation, levels = c(
"1","2","3","4","5")))
  


kableExtra::kbl(
  dep_tavi_rates_per %>% select(-Group),
    booktabs = T, 
    caption=("Deprivation TAVI Rates per 100,000 per year held at mean TAVI age group"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2005 - 2009", 1, 5) %>%
    group_rows("2010 - 2014", 6, 10) %>%
    group_rows("2015 - 2019", 11, 15) %>%
    group_rows("2020 - 2023", 16, 20)


```


```{r, dep_results_plot_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}

# Plot
dep_plot_df_tavi <- dep_tavi_rates_per_raw %>% mutate(surgery_type="TAVI")

dep_plot_df_tavi %>%
  rename(n=predicted,treatment_time=Group) %>%
  ggplot(aes(x=treatment_time, y=n, group=Deprivation, color=Deprivation)) + theme_bw()+
   facet_wrap(surgery_type ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at average age) over Time',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793"))


```




```{r, dep_results_plot2_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}


dep_plot_df_tavi %>%
  filter(Group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=Group) %>%
  mutate(dummy="1") %>%
  ggplot(aes(x=dummy, y=n, group=Deprivation, color=Deprivation)) + theme_bw()+
   facet_wrap(treatment_time ~ ., nrow=1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at mean age) for pre, preceding and during pandemic periods',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793")) +
  theme(axis.text.x = element_blank(),
        panel.grid.major = element_blank(),
    panel.grid.minor = element_blank())



```

#### Predicted rates per year per 100,000 people - all age groups

```{r, dep_results_plot3_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

ggemmeans(dep_models_list[[2]], c('Deprivation','treatment_time', 'age_group')) %>%
  as.data.frame() %>%
  mutate(predicted=predicted*exp(13.17)/100000) %>%
  mutate(conf.low=conf.low*exp(13.17)/100000) %>%
  mutate(conf.high=conf.high*exp(13.17)/100000) %>%
  filter(group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=group,age=facet,Deprivation=x) %>%
  ggplot(aes(x=age, y=n, group=Deprivation, color=Deprivation)) + theme_bw()+
   facet_wrap(treatment_time ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("1"="#9D062A",
                                       
                                           "2"="#E56B42",
                                         
                                           "3"="#EDD587",
                                          
                                           "4"="#A0CCDE",
                                       
                                           "5"="#333793")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
  

```


#### Contrasts

Setting Quintile 1 as the reference group

```{r, dep_working_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = dep_models_list[[2]]

Deprivation_test_labels = 
hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Deprivation) %>%
  mutate(test=paste0("b",row_number()))



dep_tavi_custom_t2 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019]"), scale="irr", test="b5=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Deprivation_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)



dep_tavi_custom_t3 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b3=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b4=b1") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2020-2023]"), scale="irr", test="b5=b1") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Deprivation_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


dep_tavi_custom = 
dep_tavi_custom_t2 %>% rbind(dep_tavi_custom_t3) 


kableExtra::kbl(
  dep_tavi_custom %>% select(-treatment_time),
    booktabs = T, 
    caption=("Deprivation TAVI Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    
    group_rows("2015 - 2019", 1, 4) %>%
    group_rows("2020 - 2023", 5, 8)



```


##### Differences


```{r, dep_working_tavi_diss, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = dep_models_list[[2]]


Deprivation_test_labels_t2 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Deprivation) %>%
  mutate(test=paste0("b",row_number()))



dep_tavi_custom_contrasts_t2 = 
hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b5-(b1))=(b6-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b7-(b1))=(b8-(b2))") %>%
      as.data.frame()
    ) %>% rbind(
    hypothesis_test(model, c("Deprivation", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b9-(b1))=(b10-(b2))") %>%
      as.data.frame()
    ) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(Deprivation_test_labels_t2,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Deprivation, " - 1 Difference")) %>%
  select(-Deprivation) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019 - 2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


dep_tavi_custom_contrasts = 
dep_tavi_custom_contrasts_t2


kableExtra::kbl(
  (dep_tavi_custom_contrasts%>%select(-treatment_time)),
    booktabs = T, 
    caption=("Deprivation TAVI Differences Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015-2019 - 2020-2023", 1, 4)

```




#### Relative Risks

##### Unadjusted

```{r, dep_working_tavi_irr_unadj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

dep_access_to_survery_df = dep_access_to_survery_df %>% mutate(Deprivation=factor(Deprivation,
                                           levels=c("1","2","3","4","5")))
  


dep_model_2015_unadj = glm(n ~ Deprivation  + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

dep_irr_2015 = exp(coef(dep_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))



dep_model_2020_unadj = glm(n ~ Deprivation  + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

dep_irr_2020 = exp(coef(dep_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))

dep_irr_unadj = dep_irr_2015 %>% mutate(treatment_time="2015-2019") %>%
  
  rbind(dep_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  dep_irr_unadj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Deprivation TAVI Relative Risk - Unadjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015 - 2019", 1, 4) %>%
    group_rows("2020 - 2023", 5, 8)

```


##### Adjusted

```{r, dep_working_tavi_irr_adj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

dep_access_to_survery_df = dep_access_to_survery_df %>% mutate(Deprivation=factor(Deprivation,
                                           levels=c("1","2","3","4","5")))

dep_model_2015_adj = glm(n ~ Deprivation + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

dep_irr_2015 = exp(coef(dep_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))



dep_model_2020_adj = glm(n ~ Deprivation + age_group + offset((population)),
                           data = (subset(dep_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

dep_irr_2020 = exp(coef(dep_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(dep_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Deprivation=parameter) %>%
  mutate(Deprivation=str_replace(Deprivation,"Deprivation",""))

dep_irr_adj = dep_irr_2015 %>% mutate(treatment_time="2015-2019") %>%

  rbind(dep_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  dep_irr_adj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Deprivation TAVI Relative Risk - Adjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015 - 2019", 1, 4) %>%
    group_rows("2020 - 2023", 5, 8)

```


## Sex

Unknown Sex group removed.

Model: Poisson GLM with log population offset.



```{r, sex_working, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}
 
sex_access_to_survery_df = 
access_to_survery_df %>%
  filter(Sex!="Unknown") %>%
  group_by(surgery_type,Sex,age_group,treatment_time) %>% count() %>%
  left_join((pop_ethnicity_mapped_sex_age%>%select(-YEAR)),
            by = join_by("Sex"=="SEX", age_group)) %>%
  rename(population=pop_ethnicity_var_sex_age) %>%
  mutate(population=log(population)) %>%
  #now work out count average in the treatment time years
  mutate(treatment_time_length = ifelse(treatment_time=="2020-2023",4,5)) %>%
  mutate(n=n/treatment_time_length) %>%
  mutate(Sex=factor(Sex,levels=c("Male","Female")))



# Models - Fit for SAVR and TAVI separately
# Modelling Sex and Treatment time with their interaction
# Controlling for Age Group
# Offsetting the count by the population
sex_models_list = list()

sex_models_list[[1]] = glm(n ~ Sex + treatment_time + Sex * treatment_time + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "SAVR")),
                           family = "poisson")

sex_models_list[[2]] = glm(n ~ Sex + treatment_time + Sex * treatment_time + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "TAVI")),
                           family = "poisson")


# Calculate marginal means, interaction effects, and contrasts for each surgery_type
sex_results_list <- lapply(sex_models_list, function(model) {
  
  # Calculate marginal means - the average response (here, rates n) for each combination of the predictor variables (Sex and treatment_time), while averaging over the other variables (age_group).
  
  #sex_emm <- emmeans(model, ~ Sex * treatment_time,type = "response")
  sex_emm <- ggemmeans(model, c('Sex','treatment_time'))

  # Extract interaction effects - the estimated marginal means for Sex within each level of treatment_time. E.g. How the interaction effect and how the rates vary between Sex groups (e,g, White Black Asian etc) for each treatment_time period.
  # sex_inter_emm <- emmeans(model, ~ Sex | treatment_time,type = "response")

  # Create contrasts - specific pairwise comparisons for the interaction effect, considering the control for age -  indicating whether there are significant differences in rates between each Sex group for each time period, while controlling for age.
  #sex_contrasts <- contrast(sex_inter_emm, method = "pairwise")
  sex_contrasts <- hypothesis_test(model, c("Sex", "treatment_time"), type="irr")
  
  

  # Store results in a list
  results <- list(emm = sex_emm, contrasts = sex_contrasts)
  return(results)
})

# Results for each surgery type
sex_results_savr <- sex_results_list[[1]]
sex_results_tavi <- sex_results_list[[2]]


```



### SAVR

#### Predicted rates per year per 100,000 people - held at mean age

Held at age group: 70-74 (mean age of SAVR is 70)

```{r, sex_working_savr_rates_population, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

# running this to see what the population is adjusted for - for working in per 100,000
ggemmeans(sex_models_list[[1]], c('Sex','treatment_time', 'age_group [70-74]'))

# Adjusted for population 14.13

```

```{r, sex_working_savr_rates_per, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


sex_savr_rates_per_raw = 
ggemmeans(sex_models_list[[1]], c('Sex','treatment_time', 'age_group [70-74]')) %>% 
  as.data.frame() %>%
  # per 100000
  mutate(predicted=predicted*exp(14.13)/100000) %>%
  mutate(conf.low=conf.low*exp(14.13)/100000) %>%
  mutate(conf.high=conf.high*exp(14.13)/100000) %>%
  rename(Group=group,Sex=x)
  
sex_savr_rates_per = 
  sex_savr_rates_per_raw %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Predicted = format(round(predicted, digits = 2), nsmall = 2)) %>%
  select(Group,Sex,Predicted,`95% CI`) %>%
  arrange(Group, factor(Sex, levels = c(
"Male","Female")))
  


kableExtra::kbl(
  sex_savr_rates_per %>% select(-Group),
    booktabs = T, 
    caption=("Sex SAVR Rates per 100,000 per year held at mean SAVR age group"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 2) %>%
    group_rows("2005 - 2009", 3, 4) %>%
    group_rows("2010 - 2014", 5, 6) %>%
    group_rows("2015 - 2019", 7, 8) %>%
    group_rows("2020 - 2023", 9, 10)


```


```{r, sex_results_plot, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}

# Plot
sex_plot_df <- sex_savr_rates_per_raw %>% mutate(surgery_type="SAVR")

sex_plot_df %>%
  rename(n=predicted,treatment_time=Group) %>%
  ggplot(aes(x=treatment_time, y=n, group=Sex, color=Sex)) + theme_bw()+
   facet_wrap(surgery_type ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at average age) over Time',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22"))


```




```{r, sex_results_plot2, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}


sex_plot_df %>%
  filter(Group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=Group) %>%
  mutate(dummy="1") %>%
  ggplot(aes(x=dummy, y=n, group=Sex, color=Sex)) + theme_bw()+
   facet_wrap(treatment_time ~ ., nrow=1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at mean age) for pre, preceding and during pandemic periods',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22")) +
  theme(axis.text.x = element_blank(),
        panel.grid.major = element_blank(),
    panel.grid.minor = element_blank())



```

#### Predicted rates per year per 100,000 people - all age groups

```{r, sex_results_plot3, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

ggemmeans(sex_models_list[[1]], c('Sex','treatment_time', 'age_group')) %>%
  as.data.frame() %>%
  mutate(predicted=predicted*exp(14.13)/100000) %>%
  mutate(conf.low=conf.low*exp(14.13)/100000) %>%
  mutate(conf.high=conf.high*exp(14.13)/100000) %>%
  filter(group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=group,age=facet,Sex=x) %>%
  ggplot(aes(x=age, y=n, group=Sex, color=Sex)) + theme_bw()+
   facet_wrap(treatment_time ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
  

```



#### Contrasts

Setting White as the reference group

```{r, sex_working_savr, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = sex_models_list[[1]]

Sex_test_labels = 
hypothesis_test(model, c("Sex", "treatment_time [2000-2004]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Sex) %>%
  mutate(test=paste0("b",row_number()))


# Sex - each level vs Rest in 2000-2004 then join on other time periods
sex_savr_custom_t1 = 
hypothesis_test(model, c("Sex", "treatment_time [2000-2004]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Sex_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


sex_savr_custom_t2 = 
hypothesis_test(model, c("Sex", "treatment_time [2015-2019]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Sex_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)



sex_savr_custom_t3 = 
hypothesis_test(model, c("Sex", "treatment_time [2020-2023]"), scale="irr", test="b2=b1") %>%
  as.data.frame()  %>% 
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Sex_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


sex_savr_custom = 
sex_savr_custom_t1 %>% rbind(sex_savr_custom_t2) %>% rbind(sex_savr_custom_t3)


kableExtra::kbl(
  sex_savr_custom %>% select(-treatment_time),
    booktabs = T, 
    caption=("Sex SAVR Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 1) %>%
    group_rows("2015 - 2019", 2, 2) %>%
    group_rows("2020 - 2023", 3,3)



```


##### Differences

Now considering differences:

eg Asian-White difference in 2000-2004 compared to 2015-2019
Asian-White difference in 2015-2019 compared to 2020-2023


```{r, sex_working_savr_diss, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = sex_models_list[[1]]

Sex_test_labels_t1 = 
hypothesis_test(model, c("Sex", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Sex) %>%
  mutate(test=paste0("b",row_number()))

Sex_test_labels_t2 = 
hypothesis_test(model, c("Sex", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Sex) %>%
  mutate(test=paste0("b",row_number()))



sex_savr_custom_contrasts_t1 = 
hypothesis_test(model, c("Sex", "treatment_time [2000-2004, 2015-2019]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(Sex_test_labels_t1,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male Difference")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2000-2004 - 2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


sex_savr_custom_contrasts_t2 = 
hypothesis_test(model, c("Sex", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(Sex_test_labels_t2,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male Difference")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019 - 2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


sex_savr_custom_contrasts = 
sex_savr_custom_contrasts_t1 %>% rbind(sex_savr_custom_contrasts_t2)


kableExtra::kbl(
  (sex_savr_custom_contrasts%>%select(-treatment_time)),
    booktabs = T, 
    caption=("Sex SAVR Differences Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000-2004 - 2015-2019", 1, 1) %>%
    group_rows("2015-2019 - 2020-2023", 2, 2)

```




#### Relative Risks

##### Unadjusted

```{r, sex_working_savr_irr_unadj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

sex_access_to_survery_df = sex_access_to_survery_df %>% mutate(Sex=factor(Sex,
                                           levels=c("Male","Female")))
  
sex_model_2000_unadj = glm(n ~ Sex +  offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2000-2004")),
                           family = "poisson")

sex_irr_2000 = exp(coef(sex_model_2000_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2000_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))


sex_model_2015_unadj = glm(n ~ Sex  + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

sex_irr_2015 = exp(coef(sex_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))



sex_model_2020_unadj = glm(n ~ Sex  + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

sex_irr_2020 = exp(coef(sex_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))

sex_irr_unadj = sex_irr_2000 %>% mutate(treatment_time="2000-2004") %>%
  rbind(sex_irr_2015 %>% mutate(treatment_time="2015-2019")) %>%
  rbind(sex_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  sex_irr_unadj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Sex SAVR Relative Risk - Unadjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 1) %>%
    group_rows("2015 - 2019", 2,2) %>%
    group_rows("2020 - 2023", 3,3)

```


##### Adjusted

```{r, sex_working_savr_irr_adj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

sex_access_to_survery_df = sex_access_to_survery_df %>% mutate(Sex=factor(Sex,
                                           levels=c("Male","Female")))
  
sex_model_2000_adj = glm(n ~ Sex + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2000-2004")),
                           family = "poisson")

sex_irr_2000 = exp(coef(sex_model_2000_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2000_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))


sex_model_2015_adj = glm(n ~ Sex + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

sex_irr_2015 = exp(coef(sex_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))



sex_model_2020_adj = glm(n ~ Sex + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "SAVR")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

sex_irr_2020 = exp(coef(sex_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))

sex_irr_adj = sex_irr_2000 %>% mutate(treatment_time="2000-2004") %>%
  rbind(sex_irr_2015 %>% mutate(treatment_time="2015-2019")) %>%
  rbind(sex_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  sex_irr_adj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Sex SAVR Relative Risk - Adjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2000 - 2004", 1, 1) %>%
    group_rows("2015 - 2019", 2,2) %>%
    group_rows("2020 - 2023", 3,3)

```




### TAVI

#### Predicted rates per year per 100,000 people - held at mean age

Held at age group: 80-84 (mean age of TAVI is 80)

```{r, sex_working_tavi_rates_population, warning=FALSE, error=FALSE, message=FALSE, eval=FALSE, echo=FALSE}

sex_models_list[[2]] = glm(n ~ Sex + treatment_time + Sex * treatment_time + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "TAVI")),
                           family = "poisson")

# running this to see what the population is adjusted for - for working in per 100,000
ggemmeans(sex_models_list[[2]], c('Sex','treatment_time', 'age_group [80-84]'))

# Adjusted for population 14.03

```

```{r, sex_working_tavi_rates_per, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}


sex_tavi_rates_per_raw = 
ggemmeans(sex_models_list[[2]], c('Sex','treatment_time', 'age_group [80-84]')) %>% 
  as.data.frame() %>%
  # per 100000
  mutate(predicted=predicted*exp(14.03)/100000) %>%
  mutate(conf.low=conf.low*exp(14.03)/100000) %>%
  mutate(conf.high=conf.high*exp(14.03)/100000) %>%
  rename(Group=group,Sex=x)
  
sex_tavi_rates_per = 
  sex_tavi_rates_per_raw %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Predicted = format(round(predicted, digits = 2), nsmall = 2)) %>%
  select(Group,Sex,Predicted,`95% CI`) %>%
  arrange(Group, factor(Sex, levels = c(
"Male","Female")))
  


kableExtra::kbl(
  sex_tavi_rates_per %>% select(-Group),
    booktabs = T, 
    caption=("Sex TAVI Rates per 100,000 per year held at mean TAVI age group"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    group_rows("2005 - 2009", 1, 2) %>%
    group_rows("2010 - 2014", 3, 4) %>%
    group_rows("2015 - 2019", 5, 6) %>%
    group_rows("2020 - 2023", 7, 8)


```


```{r, sex_results_plot_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}

# Plot
sex_plot_df_tavi <- sex_tavi_rates_per_raw %>% mutate(surgery_type="TAVI")

sex_plot_df_tavi %>%
  rename(n=predicted,treatment_time=Group) %>%
  ggplot(aes(x=treatment_time, y=n, group=Sex, color=Sex)) + theme_bw()+
   facet_wrap(surgery_type ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at average age) over Time',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22"))


```




```{r, sex_results_plot2_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=6}


sex_plot_df_tavi %>%
  filter(Group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=Group) %>%
  mutate(dummy="1") %>%
  ggplot(aes(x=dummy, y=n, group=Sex, color=Sex)) + theme_bw()+
   facet_wrap(treatment_time ~ ., nrow=1) +
    geom_line(size=1,position=position_dodge(0.2)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means - Rates (held at mean age) for pre, preceding and during pandemic periods',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22")) +
  theme(axis.text.x = element_blank(),
        panel.grid.major = element_blank(),
    panel.grid.minor = element_blank())



```

#### Predicted rates per year per 100,000 people - all age groups

```{r, sex_results_plot3_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE, fig.width=12,fig.height=10}

ggemmeans(sex_models_list[[2]], c('Sex','treatment_time', 'age_group')) %>%
  as.data.frame() %>%
  mutate(predicted=predicted*exp(14.03)/100000) %>%
  mutate(conf.low=conf.low*exp(14.03)/100000) %>%
  mutate(conf.high=conf.high*exp(14.03)/100000) %>%
  filter(group %in% c("2000-2004","2015-2019","2020-2023")) %>%
  rename(n=predicted,treatment_time=group,age=facet,Sex=x) %>%
  ggplot(aes(x=age, y=n, group=Sex, color=Sex)) + theme_bw()+
   facet_wrap(treatment_time ~ ., scales='free', ncol= 1) +
    geom_line(size=1,position=position_dodge(0)) +
    geom_point(size=2,position=position_dodge(0.2))+
    geom_errorbar(aes(ymin=conf.low, ymax=conf.high), width=0.2, size=1.25,
                  position=position_dodge(0.2)) +
    labs(title='95% CI of Estimated Marginal Means',x="",y="Predicted count per 100,000 people per year") +
  scale_color_manual(name = "", values = c("Female" = "#33ADBA", "Male" = "#FD7E22")) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
  

```


#### Contrasts

Setting Male as the reference group

```{r, sex_working_tavi, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = sex_models_list[[2]]

Sex_test_labels = 
hypothesis_test(model, c("Sex", "treatment_time [2015-2019]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Sex) %>%
  mutate(test=paste0("b",row_number()))



sex_tavi_custom_t2 = 
hypothesis_test(model, c("Sex", "treatment_time [2015-2019]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Sex_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)



sex_tavi_custom_t3 = 
hypothesis_test(model, c("Sex", "treatment_time [2020-2023]"), scale="irr", test="b2=b1") %>%
  as.data.frame() %>% 
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  left_join(Sex_test_labels,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


sex_tavi_custom = 
sex_tavi_custom_t2 %>% rbind(sex_tavi_custom_t3) 


kableExtra::kbl(
  sex_tavi_custom %>% select(-treatment_time),
    booktabs = T, 
    caption=("Sex TAVI Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%
    
    group_rows("2015 - 2019", 1, 1) %>%
    group_rows("2020 - 2023", 2,2)



```


##### Differences


```{r, sex_working_tavi_diss, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

model = sex_models_list[[2]]


Sex_test_labels_t2 = 
hypothesis_test(model, c("Sex", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test=NULL)%>%as.data.frame()%>%
  select(Sex) %>%
  mutate(test=paste0("b",row_number()))



sex_tavi_custom_contrasts_t2 = 
hypothesis_test(model, c("Sex", "treatment_time [2015-2019, 2020-2023]"), scale="irr", test="(b3-(b1))=(b4-(b2))") %>%
  as.data.frame() %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^=]+")) %>%
  mutate(Hypothesis=str_remove(Hypothesis,"\\(")) %>%
  mutate(Hypothesis=str_extract(Hypothesis, "^[^-]+")) %>%
  left_join(Sex_test_labels_t2,by=c("Hypothesis"="test")) %>%
  mutate(Hypothesis=paste0(Sex, " - Male Difference")) %>%
  select(-Sex) %>%
  mutate(`95% CI` = paste0( format(round(conf.low, digits = 2), nsmall = 2),
                             ", ",
                             format(round(conf.high, digits = 2), nsmall = 2)
                             )) %>%
  mutate(Contrast = format(signif(Contrast, 2), scientific = FALSE)) %>%
  mutate(p = ifelse(p.value<0.001, "< .001", as.character(format(round(p.value, digits = 3), nsmall = 3)))) %>%
  mutate(treatment_time = "2015-2019 - 2020-2023") %>%
  select(Hypothesis,treatment_time,Contrast,`95% CI`,p)


sex_tavi_custom_contrasts = 
sex_tavi_custom_contrasts_t2


kableExtra::kbl(
  (sex_tavi_custom_contrasts%>%select(-treatment_time)),
    booktabs = T, 
    caption=("Sex TAVI Differences Pairwise Custom Contrasts"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015-2019 - 2020-2023", 1, 1)

```




#### Relative Risks

##### Unadjusted

```{r, sex_working_tavi_irr_unadj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

sex_access_to_survery_df = sex_access_to_survery_df %>% mutate(Sex=factor(Sex,
                                           levels=c("Male","Female")))
  


sex_model_2015_unadj = glm(n ~ Sex  + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

sex_irr_2015 = exp(coef(sex_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2015_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))



sex_model_2020_unadj = glm(n ~ Sex  + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

sex_irr_2020 = exp(coef(sex_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2020_unadj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))

sex_irr_unadj = sex_irr_2015 %>% mutate(treatment_time="2015-2019") %>%
  
  rbind(sex_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  sex_irr_unadj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Sex TAVI Relative Risk - Unadjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015 - 2019", 1, 1) %>%
    group_rows("2020 - 2023", 2,2)

```


##### Adjusted

```{r, sex_working_tavi_irr_adj, warning=FALSE, error=FALSE, message=FALSE, eval=TRUE, echo=FALSE}

sex_access_to_survery_df = sex_access_to_survery_df %>% mutate(Sex=factor(Sex,
                                           levels=c("Male","Female")))

sex_model_2015_adj = glm(n ~ Sex + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2015-2019")),
                           family = "poisson")

sex_irr_2015 = exp(coef(sex_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2015_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))



sex_model_2020_adj = glm(n ~ Sex + age_group + offset((population)),
                           data = (subset(sex_access_to_survery_df, surgery_type == "TAVI")%>%filter(treatment_time=="2020-2023")),
                           family = "poisson")

sex_irr_2020 = exp(coef(sex_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter") %>%
  rename("IRR"=".") %>%
  left_join(exp(confint.default(sex_model_2020_adj)) %>% as.data.frame() %>% rownames_to_column("parameter"),by = join_by(parameter)) %>%
  
  mutate(`95% CI` = paste0( format(round(`2.5 %`, digits = 4), nsmall = 4),
                             ", ",
                             format(round(`97.5 %`, digits = 4), nsmall = 4)
                             )) %>%
  mutate(IRR = format(round(IRR, digits = 4), nsmall = 4)) %>%
  select(parameter,IRR,`95% CI`) %>%
  filter(!str_detect(parameter,"age_group")) %>%
  filter(!str_detect(parameter,"Intercept")) %>%
  rename(Sex=parameter) %>%
  mutate(Sex=str_replace(Sex,"Sex",""))

sex_irr_adj = sex_irr_2015 %>% mutate(treatment_time="2015-2019") %>%

  rbind(sex_irr_2020 %>% mutate(treatment_time="2020-2023"))

kableExtra::kbl(
  sex_irr_adj %>% select(-treatment_time),
    booktabs = T, 
    caption=("Sex TAVI Relative Risk - Adjusted"),
    linesep = "",
    align="l"
    ) %>%
  kableExtra::kable_styling(
    #font_size
    ) %>%

    group_rows("2015 - 2019", 1, 1) %>%
    group_rows("2020 - 2023", 2,2)

```





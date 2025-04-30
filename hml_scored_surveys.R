###############################################
#                                             #
#  PAN accessing the database remotely        #
#                                             #
###############################################
#
# Mar 2025
# vpfeifer@arizona.edu
#
##############################################################################
# install.packages("RPostgres")
# install.packages("DBI")

require(DBI)
require(RPostgres)
library(dplyr)

############### connect to database

con <- dbConnect(RPostgres::Postgres(),
                 user = Sys.getenv("user")
                 , password = Sys.getenv("password")
                 , dbname = Sys.getenv("dbname")
                 , host = Sys.getenv("host")
                 , port = Sys.getenv("port")
                 , sslrootcert = 'global-bundle.pem'
)


##############################################################################
####################### How to Pull Data #####################################
# need to look up data table names in data dictionary

demo <- dbReadTable(con, "p2_redcap_demographics")

##############################################################################
#### STRESS ####
stress <- dbReadTable(con, "p2_redcap_perceived_stress")
summary(stress)

### checking how many completed surveys we have
stress$na <- rowSums(is.na(stress[5:14]))
summary(as.factor(stress$na))
#### one could clean some of the partially completed ones if needed

### How to score stress survey: total all questions, except
### Questions 4, 5, 7, 8, they need to be reverse scored
### 0 = 4, 1 = 3, 2 = 2, 3 = 1, 4 = 0

# recoding the data that needs reverse scoring
stress$q4 <- 4 - stress$stress_v104
stress$q5 <- 4 - stress$stress_v105
stress$q7 <- 4 - stress$stress_v107
stress$q8 <- 4 - stress$stress_v108

# summing up all questions for final score
stress$stress_total <- stress$stress_v101 + stress$stress_v102 + stress$stress_v103 +
  stress$stress_v106 + stress$stress_v109 + stress$stress_v1010 + stress$q4 + stress$q5 +
  stress$q7 + stress$q8

summary(as.factor(stress$stress_total))
##############################################################################
social_stressor <- dbReadTable(con, "p2_redcap_social_stressor")
summary(social_stressor)

################################
### question logic: if Q1 is answered "yes", Q1_yes is a 1-3 scale intensity rating

#### extracting events only 
social_stressor$nr_of_stressful_events <- social_stressor$socstress_v101 + 
                              social_stressor$socstress_v102 + 
                              social_stressor$socstress_v103 + 
                              social_stressor$socstress_v104 + 
                              social_stressor$socstress_v105 + 
                              social_stressor$socstress_v106 + 
                              social_stressor$socstress_v107 + 
                              social_stressor$socstress_v108 + 
                              social_stressor$socstress_v109 + 
                              social_stressor$socstress_v1010 + 
                              social_stressor$socstress_v1011 + 
                              social_stressor$socstress_v1012

summary(as.factor(social_stressor$nr_of_stressful_events))
####################################################################
# need to put events and follow up Questions together

# NA in follow up questions can mean this question was never asked

social_stressor$q1 <- ifelse(social_stressor$socstress_v101==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v101_yes),1,
                           social_stressor$socstress_v101_yes)))
social_stressor$q2 <- ifelse(social_stressor$socstress_v102==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v102_yes),1, 
                           social_stressor$socstress_v102_yes)))
social_stressor$q3 <- ifelse(social_stressor$socstress_v103==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v103_yes),1, 
                           social_stressor$socstress_v103_yes)))
social_stressor$q4 <- ifelse(social_stressor$socstress_v104==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v104_yes),1, 
                           social_stressor$socstress_v104_yes)))
social_stressor$q5 <- ifelse(social_stressor$socstress_v105==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v105_yes),1, 
                           social_stressor$socstress_v105_yes)))
social_stressor$q6 <- ifelse(social_stressor$socstress_v106==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v106_yes),1, 
                           social_stressor$socstress_v106_yes)))
social_stressor$q7 <- ifelse(social_stressor$socstress_v107==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v107_yes),1, 
                           social_stressor$socstress_v107_yes)))
social_stressor$q8 <- ifelse(social_stressor$socstress_v108==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v108_yes),1, 
                           social_stressor$socstress_v108_yes)))
social_stressor$q9 <- ifelse(social_stressor$socstress_v109==0, 0, 
                   (ifelse(is.na(social_stressor$socstress_v109_yes),1, 
                           social_stressor$socstress_v109_yes)))
social_stressor$q10 <- ifelse(social_stressor$socstress_v1010==0, 0, 
                    (ifelse(is.na(social_stressor$socstress_v1010_yes),1, 
                            social_stressor$socstress_v1010_yes)))
social_stressor$q11 <- ifelse(social_stressor$socstress_v1011==0, 0, 
                    (ifelse(is.na(social_stressor$socstress_v1011_yes),1, 
                            social_stressor$socstress_v1011_yes)))
social_stressor$q12 <- ifelse(social_stressor$socstress_v1012==0, 0, 
                    (ifelse(is.na(social_stressor$socstress_v1012_yes),1, 
                            social_stressor$socstress_v1012_yes)))

social_stressor$socstress_total <- social_stressor$q1 +
                                          social_stressor$q2 +
                                          social_stressor$q3 +
                                          social_stressor$q4 + 
                                          social_stressor$q5 +
                                          social_stressor$q6 + 
                                          social_stressor$q7 +
                                          social_stressor$q8 +
                                          social_stressor$q9 +
                                          social_stressor$q10 +
                                          social_stressor$q11 +
                                          social_stressor$q12

summary(as.factor(social_stressor$socstress_total))
##############################################################################
social_support <- dbReadTable(con, "p2_redcap_social_support")

############### Scoring
# This is two surveys, loneliness and Multidimensional Scale of Perceived Social Support

# Loneliness: sum of three items (socsupp_v101, 102, 103) 

# MSPSS: sum of all twelve items (socsupp_v104:1015)
# Significant Other Subscale: Sum across items 1, 2, 5, & 10, divide by 4
# Family Subscale: Sum across items 3, 4, 8, & 11, divide by 4
# Friends Subscale: Sum across items 6, 7, 9, & 12, divide by 4

#ignores incomplete cases / produces NAs for incomplete data
social_support$loneliness_total <- (social_support$socsupp_v101 +
                                      social_support$socsupp_v102 +
                                      social_support$socsupp_v103)

summary(as.factor(social_support$loneliness_total))


social_support$MSPSS_total <- (social_support$socsupp_v104 +
                                 social_support$socsupp_v105 +
                                 social_support$socsupp_v106 + 
                                 social_support$socsupp_v107 + 
                                 social_support$socsupp_v108 +
                                 social_support$socsupp_v109 +
                                 social_support$socsupp_v1010 + 
                                 social_support$socsupp_v1011 + 
                                 social_support$socsupp_v1012 +
                                 social_support$socsupp_v1013 + 
                                 social_support$socsupp_v1014 + 
                                 social_support$socsupp_v1015)
summary(as.factor(social_support$MSPSS_total))


social_support$MSPSS_family <- (social_support$socsupp_v106 + 
                                  social_support$socsupp_v107 +
                                  social_support$socsupp_v1011 + 
                                  social_support$socsupp_v1014)/4

social_support$MSPSS_friends <- (social_support$socsupp_v109 + 
                                   social_support$socsupp_v1010 +
                                   social_support$socsupp_v1012 + 
                                   social_support$socsupp_v1015)/4

social_support$MSPSS_so <- (social_support$socsupp_v104 + 
                              social_support$socsupp_v105 +
                              social_support$socsupp_v108 +
                              social_support$socsupp_v1013)/4


###############################################################################
swls <- dbReadTable(con, "p2_redcap_swls")
summary(swls)

# scored as a single sum
swls$swls_total <- (swls$swls_v101 + swls$swls_v102 + 
                      swls$swls_v103 + swls$swls_v104 + swls$swls_v105)


##############################################################################
#####################################
anx <- dbReadTable(con, "p2_redcap_anxiety")
qpar <- dbReadTable(con, "p2_redcap_qpar")
sleep <- dbReadTable(con, "p2_redcap_sleep")
#####################################

psych::describe(anx)
summary(anx)
# DATA STRUCTURE: 
# first question: intensity, second question: frequency
# odd questions: intensity; even questions: frequency
# scoring: add all frequency, all intensity, then add up

anx$anxiety_intensity <- anx$anx_v101 + anx$anx_v103 + anx$anx_v105 + anx$anx_v107 + 
  anx$anx_v109 + anx$anx_v1011 + anx$anx_v1013 + anx$anx_v1015 + anx$anx_v1017 + 
  anx$anx_v1019 + anx$anx_v1021 + anx$anx_v1023 + anx$anx_v1025 + anx$anx_v1027 + 
  anx$anx_v1029 + anx$anx_v1031 + anx$anx_v1033

anx$anxiety_frequency <- anx$anx_v102 + anx$anx_v104 + anx$anx_v106 + anx$anx_v108 + 
  anx$anx_v1010 + anx$anx_v1012 + anx$anx_v1014 + anx$anx_v1016 + anx$anx_v1018 + 
  anx$anx_v1020 + anx$anx_v1022 + anx$anx_v1024 + anx$anx_v1026 + anx$anx_v1028 + 
  anx$anx_v1030 + anx$anx_v1032 + anx$anx_v1034

anx$anxiety_total <- anx$anxiety_frequency + anx$anxiety_intensity
summary(as.factor(anx$anx_all))
plot(anx$anxiety_frequency, anx$anxiety_intensity)

########################## Q PAR ##############################################
summary(qpar)

qpar$qpar_v109 <- ifelse(qpar$qpar_v101 == 0, 0, qpar$qpar_v109)
qpar$qpar_v1010 <- ifelse(qpar$qpar_v102 == 0, 0, qpar$qpar_v1010)
qpar$qpar_v1011 <- ifelse(qpar$qpar_v103 == 0, 0, qpar$qpar_v1011)
qpar$qpar_v1012 <- ifelse(qpar$qpar_v104 == 0, 0, qpar$qpar_v1012)
qpar$qpar_v1013<- ifelse(qpar$qpar_v105 == 0, 0, qpar$qpar_v1013)
qpar$qpar_v1014 <- ifelse(qpar$qpar_v106 == 0, 0, qpar$qpar_v1014)
qpar$qpar_v1015 <- ifelse(qpar$qpar_v107 == 0, 0, qpar$qpar_v1015)
qpar$qpar_v1016 <- ifelse(qpar$qpar_v108 == 0, 0, qpar$qpar_v1016)

# Recode duration scores:
# REDCap factor levels are 0-2, but QPAR coding needs to be 1-3, 
# so 1 = less than an hour, 2 = 1-2 hours, 3 = more than 3 hours.
qpar$qpar_v109 <- qpar$qpar_v109 + 1
qpar$qpar_v1010 <- qpar$qpar_v1010 + 1
qpar$qpar_v1011 <- qpar$qpar_v1011 + 1
qpar$qpar_v1012 <- qpar$qpar_v1012 + 1
qpar$qpar_v1013 <- qpar$qpar_v1013 + 1
qpar$qpar_v1014 <- qpar$qpar_v1014 + 1
qpar$qpar_v1015 <- qpar$qpar_v1015 + 1
qpar$qpar_v1016 <- qpar$qpar_v1016 + 1

#### should be 10 Qs with days per week and hours per day.
# is only 8 Q's.
# this is already pre-scored into 0-3 and 1-3 scores
qpar$dose1 <- 1 * qpar$qpar_v101 * qpar$qpar_v109
qpar$dose2 <- 1 * qpar$qpar_v102 * qpar$qpar_v1010
qpar$dose3 <- 1 * qpar$qpar_v103 * qpar$qpar_v1011
qpar$dose4 <- 2 * qpar$qpar_v104 * qpar$qpar_v1012
qpar$dose5 <- 3 * qpar$qpar_v105 * qpar$qpar_v1013
qpar$dose6 <- 2 * qpar$qpar_v106 * qpar$qpar_v1014
qpar$dose7 <- 1 * qpar$qpar_v107 * qpar$qpar_v1015
qpar$dose8 <- 1 * qpar$qpar_v108 * qpar$qpar_v1016

qpar$qpar_total <- qpar$dose1 + qpar$dose2 + qpar$dose3 + qpar$dose4 + 
  qpar$dose5 + qpar$dose6 + qpar$dose7 + qpar$dose8
qpar$qpar_mild_dose <- qpar$dose1 + qpar$dose2 + qpar$dose3 + qpar$dose7 +
  qpar$dose8
qpar$qpar_moderate_dose <- qpar$dose4 + qpar$dose6 #add missing dose 9
qpar$qpar_heavy_dose <- qpar$dose5 #add missing dose 10

summary(qpar)
psych::describe(qpar$qpar_total)
summary(as.factor(qpar$qpar_total))


######################### sleep ############################################
# psych::describe(sleep)

##########################
#### CHRONOTYPE [not scored]
#' sleep 101: morning person
#' sleep 102: insomnia
#' sleep 103 sleep disorders (general)
#' sleep v104: sleep disorders (specific)
#' sleep 106: days per week of work
#' sleep 107: bed time work / 108 bed time free time
#' sleep 1013 wake time work / 1014 wake time free time
#' 
######### M O S [scored]
#' 12 items for following constructs:
#' sleep initiation: sleep_v1026, (modified to ask about work and off days separate)
#' sleep maintenance: sleep_v1023, sleep_v1027
#' respiratory problems: sleep_v1025, sleep_v1029
#' sleep quantity (1 item) sleep_v1022
#' perceived adequacy: sleep_v1024, sleep_v1031
#' somnolence (3 items): sleep_v1028, sleep_v1030, sleep_v1032
#' 
# need to extract rating from quantity and initiation like this:
# quantitiy: 1 = 7-8 hrs, 0 = all others
# initiation: 0-15 min - more than 60 min: 1-5 score??
sleep$sleep_quantity <- sleep$sleep_v1022

sleep$sleep_initiation <- sleep$sleep_v1026
sleep$sleep_maintenance <- sleep$sleep_v1023 + sleep$sleep_v1027
sleep$sleep_respiratory_problems <- sleep$sleep_v1025 + sleep$sleep_v1029
sleep$sleep_perceived_adequacy <- sleep$sleep_v1024 + sleep$sleep_v1031
sleep$sleep_somnolence <- sleep$sleep_v1028 + sleep$sleep_v1030 + sleep$sleep_v1032

# Combine and save scored surveys ----------------------------

scored_surveys <- select(stress, hml_id, stress_total) %>%
  full_join(select(social_stressor, hml_id, nr_of_stressful_events, socstress_total), 
            by = "hml_id") %>%
  full_join(select(social_support, hml_id, loneliness_total, MSPSS_total, MSPSS_family,
                   MSPSS_friends, MSPSS_so), by = "hml_id") %>%
  full_join(select(swls, hml_id, swls_total), by = "hml_id") %>%
  full_join(select(anx, hml_id, anxiety_intensity, anxiety_frequency, anxiety_total),
            by = "hml_id") %>%
  full_join(select(sleep, hml_id, sleep_quantity, sleep_initiation, sleep_maintenance,
                   sleep_respiratory_problems, sleep_perceived_adequacy, sleep_somnolence),
            by = "hml_id") %>%
  full_join(select(qpar, hml_id, dose1, dose2, dose3, dose4, dose5, dose6, dose7, dose8, qpar_total, 
                   qpar_mild_dose, qpar_moderate_dose, qpar_heavy_dose), 
            by = "hml_id")

write.csv(scored_surveys, file = "hml_scored_surveys.csv", row.names = FALSE)

# Database section
# Creates a temp table to check for matching records
DBI::dbWriteTable(con, "temp_scored_surveys", scored_surveys, temporary = TRUE, overwrite = TRUE)

# Based on the hml_id, if it exists then update the data
# if there is no hml_id match, insert the record

# Conflict key column
key_col <- "hml_id"  

# Quote all column names
quoted_cols <- sprintf('"%s"', colnames(scored_surveys))
quoted_key_col <- sprintf('"%s"', key_col)

# Build the SET clause (excluding the key column)
update_cols <- setdiff(colnames(scored_surveys), key_col)
quoted_update_clause <- paste(sprintf('"%s" = EXCLUDED."%s"', update_cols, update_cols), collapse = ", ")

# Final SQL
sql <- sprintf("
  INSERT INTO hml_scored_surveys (%s)
  SELECT %s FROM temp_scored_surveys
  ON CONFLICT (%s) DO UPDATE SET %s
",
               paste(quoted_cols, collapse = ", "),
               paste(quoted_cols, collapse = ", "),
               quoted_key_col,
               quoted_update_clause
)

# Execute it
DBI::dbExecute(con, sql)

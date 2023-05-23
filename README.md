# Medicaid Mental Health Inpatient Hospitalizations Aggregated 

Generates a dataset of Medicaid hospitalizations with ICD codes from the supplemental information of doi:10.1001/jamapsychiatry.2021.4369

There is a column for the total number of mental health related hospitalizations (mental_health_hospitalizations) as well as a column for each type of hospitalization (eg. mood_disorders_hospitalizations, alcohol_disorders_hospitalizations). Both primary and secondary diagnoses are included in the counts.

Each row represents an aggregation by county by month stratified by combinations of age group/sex/race.

Currently contains the years 1999-2012 but will be updated as more Medicaid data is available to the lab.

### To run the code

```
export HOME_DIR=$(pwd)

cd $HOME_DIR/data/output/
mkdir /n/dominici_nsaph_l3/Lab/projects/analytic/medicaid_mental_health
ln -s /n/dominici_nsaph_l3/Lab/projects/analytic/medicaid_mental_health .
mkdir /n/dominici_nsaph_l3/Lab/data_processing/medicaid_mental_health_aggregated/scratch
ln -s /n/dominici_nsaph_l3/Lab/projects/analytic/medicaid_mental_health/scratch .

cd $HOME_DIR/src/
job01=$(generate_counts.sbatch)
```

### Data Dictionary


| Column Name | Description | Notes |
| -------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
 |year                                               | 1999-2012                                                                           | Will be updated to include more years as the Medicaid data becomes available                                                      |
| month                                              | 1-12                                                                                |                                                                                                                                   |
| state                                              | State of residence                                                                  | [https://resdac.org/cms-data/variables/state](https://resdac.org/cms-data/variables/state)                                        |
| residence_county                                   | Five digit residence county of beneficiaries                                        | FIPS5 code https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt                                                          |
| sex                                                | Sex of beneficiaries 0/1                                                            | [https://resdac.org/cms-data/variables/sex](https://resdac.org/cms-data/variables/sex)                                            |
| race                                               | Race of beneficiaries                                                               | If NA, the beneficiary had inconsistently coded race across years<br>https://resdac.org/cms-data/variables/raceethnicity-msis     |
| age_group                                          | Age group of beneficiaries                                                          | Groups: 0-17, 18-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75-84, 85+                                                                |
| mental_health_hospitalizations                     | Count of inpatient hospitalizations with codes matching any mental health condition | Matching any ICD code from the supplemental information in the following publication:<br><br>doi:10.1001/jamapsychiatry.2021.4369 |
| adjustment_reaction_hospitalizations               | Count of inpatient hospitalizations with codes matching adjustment reactions        | CCS Code 650 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| anxiety_disorders_hospitalizations                 | Count of inpatient hospitalizations with codes matching anxiety disorders           | CCS Code 651 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| attention_disorders_hospitalizations               | Count of inpatient hospitalizations with codes matching attention disorders         | CCS Code 652 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| developmental_disorders_hospitalizations           | Count of inpatient hospitalizations with codes matching developmental disorders     | CCS Code 654 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| infancy_childhood_disorders_hospitalizations       | Count of inpatient hospitalizations with codes matching infancy/childhood disorders | CCS Code 655 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| mood_disorders_hospitalizations                    | Count of inpatient hospitalizations with codes matching mood disorders              | CCS Code 657 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| personality_disorders_hospitalizations             | Count of inpatient hospitalizations with codes matching personality disorders       | CCS Code 658 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| schizophrenia_psychotic_disorders_hospitalizations | Count of inpatient hospitalizations with codes matching schizophrenia disorders     | CCS Code 659 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| alcohol_disorders_hospitalizations                 | Count of inpatient hospitalizations with codes matching alcohol disorders           | CCS Code 660 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| substance_disorders_hospitalizations               | Count of inpatient hospitalizations with codes matching any substance disorders     | CCS Code 661 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| suicide_self_harm_hospitalizations                 | Count of inpatient hospitalizations with codes matching suicide/self harm           | CCS Code 662 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| misc_disorders_hospitalizations                    | Count of inpatient hospitalizations with codes matching miscellaneous disorders     | CCS Code 670 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |

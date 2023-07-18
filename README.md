# Medicaid Mental Health Inpatient Hospitalizations Aggregated 

Generates a dataset of Medicaid hospitalizations with ICD codes from the supplemental information of doi:10.1001/jamapsychiatry.2021.4369

There is a column for the total number of mental health related hospitalizations (mental_health_hospitalizations) as well as a column for each type of hospitalization (eg. mood_disorders_hospitalizations, alcohol_disorders_hospitalizations). Both primary and secondary diagnoses are included in the counts.

Each row represents an aggregation by county by month stratified by combinations of age group/sex/race.

Currently contains the years 1999-2012 but will be updated as more Medicaid data is available to the lab.

### Data Dictionary


| Column Name | Description | Notes |
| -------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
 |year                                               | 1999-2012                                                                           | Will be updated to include more years as the Medicaid data becomes available                                                      |
| month                                              | 1-12                                                                                |                                                                                                                                   |
| state                                              | State of residence                                                                  | [https://resdac.org/cms-data/variables/state](https://resdac.org/cms-data/variables/state)                                        |
| residence_county                                   | Five digit residence county of beneficiaries                                        | FIPS5 code https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt                                                          |
| sex                                                | Sex of beneficiaries. Inconsistent race codes across years are classified as U                                                            | https://resdac.org/cms-data/variables/raceethnicity-msis                                            |
| race                                               | Race of beneficiaries. Inconsistent race codes across years are classified as 9 | https://resdac.org/cms-data/variables/raceethnicity-msis     |
| age_group                                          | Age group of beneficiaries                                                          | Groups: 0-18, 19-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75-84, 85+                                                                |
| mental_health_ hospitalizations                     | Count of inpatient hospitalizations with codes matching any mental health condition | Matching any ICD code from the supplemental information in the following publication:<br><br>doi:10.1001/jamapsychiatry.2021.4369 |
| adjustment_reaction_hospitalizations               | Count of inpatient hospitalizations with codes matching adjustment reactions        | CCS Code 650 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| anxiety_disorders_ hospitalizations                 | Count of inpatient hospitalizations with codes matching anxiety disorders           | CCS Code 651 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| attention_disorders_ hospitalizations               | Count of inpatient hospitalizations with codes matching attention disorders         | CCS Code 652 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| developmental_disorders_ hospitalizations           | Count of inpatient hospitalizations with codes matching developmental disorders     | CCS Code 654 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| infancy_childhood_disorders_ hospitalizations       | Count of inpatient hospitalizations with codes matching infancy/childhood disorders | CCS Code 655 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| mood_disorders_ hospitalizations                    | Count of inpatient hospitalizations with codes matching mood disorders              | CCS Code 657 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| personality_disorders_ hospitalizations             | Count of inpatient hospitalizations with codes matching personality disorders       | CCS Code 658 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| schizophrenia_psychotic_disorders_ hospitalizations | Count of inpatient hospitalizations with codes matching schizophrenia disorders     | CCS Code 659 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| alcohol_disorders_ hospitalizations                 | Count of inpatient hospitalizations with codes matching alcohol disorders           | CCS Code 660 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| substance_disorders_ hospitalizations               | Count of inpatient hospitalizations with codes matching any substance disorders     | CCS Code 661 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| suicide_self_harm_ hospitalizations                 | Count of inpatient hospitalizations with codes matching suicide/self harm           | CCS Code 662 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |
| misc_disorders_ hospitalizations                    | Count of inpatient hospitalizations with codes matching miscellaneous disorders     | CCS Code 670 doi:10.1001/jamapsychiatry.2021.4369                                                                                 |

## Sample head

The sample shown is filled with fake values.

```
residence_county,year,month,state,sex,race,age_group,all_cause_hospitalizations,mental_health_hospitalizations,adjustment_reaction_hospitalizations,anxiety_disorders_hospitalizations,attention_disorders_hospitalizations,developmental_disorders_hospitalizations,infancy_childhood_disorders_hospitalizations,mood_disorders_hospitalizations,personality_disorders_hospitalizations,schizophrenia_psychotic_disorders_hospitalizations,alcohol_disorders_hospitalizations,substance_disorders_hospitalizations,suicide_self_harm_hospitalizations,misc_disorders_hospitalizations
37007,2012,1,NC,F,1,19-24,10,0,0,0,0,0,0,0,0,0,0,0,0,0
37081,2012,1,NC,M,2,55-64,11,1,0,0,0,0,0,0,0,0,0,0,0,0
37083,2012,1,NC,F,2,55-64,23,0,0,0,0,0,0,0,0,0,0,0,0,0
37107,2012,1,NC,F,1,25-34,1,13,1,0,0,0,0,0,0,0,0,0,0,0
37155,2012,1,NC,F,1,25-34,31,0,0,0,0,0,0,0,0,0,0,0,0,0
37049,2012,2,NC,F,2,25-34,8,0,0,0,0,0,0,0,0,0,0,0,0,0
37051,2012,2,NC,M,2,45-54,2,1,0,0,0,0,0,0,0,0,0,0,0,0
```

## Run

Clone the repository and create a conda environment.

```bash
git clone <https://github.com/<user>/repo>
cd <repo>

conda env create -f requirements.yml
conda activate <env_name>
```

It is also possible to use `mamba`.

```bash
mamba env create -f requirements.yml
mamba activate <env_name>
```

### Entrypoints

Add symlinks to input, intermediate and output folders inside the corresponding `/data` subfolders.

For example:

```
export HOME_DIR=$(pwd)

cd $HOME_DIR/data/input/ .
ln -s <input_path> .

cd $HOME_DIR/data/output/
ln -s <output_path> .
```

The README.md files inside the `/data` subfolders contain path documentation for NSAPH internal purposes.

### Pipeline

Run the script for all years:

```
python ./src/generate_counts.py --year <year>
```

or run the pipeline:

```
snakemake --cores
```

In addition, `.sbatch` templates are provided for SLURM users. Be mindful that each HPC clusters has a different configuration and the `.sbatch` files might need to be modified accordingly. 
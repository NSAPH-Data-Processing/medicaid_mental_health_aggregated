years = list(range(1999, 2013))

rule all:
    input:
        expand("data/output/medicaid_mental_health/medicaid_mental_health_{year}.csv", year=years)

rule generate_counts:
    output:
        "data/output/medicaid_mental_health/medicaid_mental_health_{year}.csv"
    params:
        year="{year}"
    log: 
        out=".logs/generate_counts_{year}.out", 
        err=".logs/generate_counts_{year}.err"
    shell:
        "python src/generate_counts.py --year {params.year} 1> {log.out} 2> {log.err}"

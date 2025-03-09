![Version](https://img.shields.io/github/v/release/DCMLab/{{ repo_name }}?display_name=tag)
[![DOI](https://zenodo.org/badge/{{ zenodo_badge_id }}.svg)](https://doi.org/{{ concept_doi }})
![GitHub repo size](https://img.shields.io/github/repo-size/DCMLab/{{ repo_name }})
![License](https://img.shields.io/badge/license-CC%20BY--NC--SA%204.0-9cf)


This is a README file for a data repository originating from the [DCML corpus initiative](https://github.com/DCMLab/dcml_corpora)
and serves as welcome page for both 

* the GitHub repo [https://github.com/DCMLab/{{ repo_name }}](https://github.com/DCMLab/{{ repo_name }}) and the corresponding
* documentation page [https://dcmlab.github.io/{{ repo_name }}](https://dcmlab.github.io/{{ repo_name }})

For information on how to obtain and use the dataset, please refer to [this documentation page](https://dcmlab.github.io/{{ repo_name }}/introduction).

# {{ pretty_repo_name }} (A corpus of annotated scores)


## Getting the data

* download repository as a [ZIP file](https://github.com/DCMLab/{{ repo_name }}/archive/main.zip)
* download a [Frictionless Datapackage](https://specs.frictionlessdata.io/data-package/) that includes concatenations
  of the TSV files in the four folders (`measures`, `notes`, `chords`, and `harmonies`) and a JSON descriptor:
  * [{{ repo_name }}.zip](https://github.com/DCMLab/{{ repo_name }}/releases/latest/download/{{ repo_name }}.zip)
  * [{{ repo_name }}.datapackage.json](https://github.com/DCMLab/{{ repo_name }}/releases/latest/download/{{ repo_name }}.datapackage.json)
* clone the repo: `git clone https://github.com/DCMLab/{{ repo_name }}.git` 


## Data Formats

Each piece in this corpus is represented by five files with identical name prefixes, each in its own folder. 
For example, {{ example_full_title }} has the following files:

* `{{ example_subcorpus }}MS3/{{ example_fname }}.mscx`: Uncompressed MuseScore 3.6.2 file including the music and annotation labels.
* `{{ example_subcorpus }}notes/{{ example_fname }}.notes.tsv`: A table of all note heads contained in the score and their relevant features (not each of them represents an onset, some are tied together)
* `{{ example_subcorpus }}measures/{{ example_fname }}.measures.tsv`: A table with relevant information about the measures in the score.
* `{{ example_subcorpus }}chords/{{ example_fname }}.chords.tsv`: A table containing layer-wise unique onset positions with the musical markup (such as dynamics, articulation, lyrics, figured bass, etc.).
* `{{ example_subcorpus }}harmonies/{{ example_fname }}.harmonies.tsv`: A table of the included harmony labels (including cadences and phrases) with their positions in the score.

Each TSV file comes with its own JSON descriptor that describes the meanings and datatypes of the columns ("fields") it contains,
follows the [Frictionless specification](https://specs.frictionlessdata.io/tabular-data-resource/),
and can be used to validate and correctly load the described file. 

### Opening Scores

After navigating to your local copy, you can open the scores in the folder `MS3` with the free and open source score
editor [MuseScore](https://musescore.org). Please note that the scores have been edited, annotated and tested with
[MuseScore 3.6.2](https://github.com/musescore/MuseScore/releases/tag/v3.6.2). 
MuseScore 4 has since been released which renders them correctly but cannot store them back in the same format.

### Opening TSV files in a spreadsheet

Tab-separated value (TSV) files are like Comma-separated value (CSV) files and can be opened with most modern text
editors. However, for correctly displaying the columns, you might want to use a spreadsheet or an addon for your
favourite text editor. When you use a spreadsheet such as Excel, it might annoy you by interpreting fractions as
dates. This can be circumvented by using `Data --> From Text/CSV` or the free alternative
[LibreOffice Calc](https://www.libreoffice.org/download/download/). Other than that, TSV data can be loaded with
every modern programming language.

### Loading TSV files in Python

Since the TSV files contain null values, lists, fractions, and numbers that are to be treated as strings, you may want
to use this code to load any TSV files related to this repository (provided you're doing it in Python). After a quick
`pip install -U ms3` (requires Python 3.10 or later) you'll be able to load any TSV like this:

```python
import ms3

labels = ms3.load_tsv("{{ example_subcorpus }}harmonies/{{ example_fname }}.harmonies.tsv")
notes = ms3.load_tsv("{{ example_subcorpus }}notes/{{ example_fname }}.notes.tsv")
```


## Version history

See the [GitHub releases](https://github.com/DCMLab/{{ repo_name }}/releases).

## Questions, Suggestions, Corrections, Bug Reports

Please [create an issue](https://github.com/DCMLab/{{ repo_name }}/issues) and/or feel free to fork and submit pull requests.

## Cite as

> Hentschel, J., Rammos, Y., Neuwirth, M., Moss, F. C., & Rohrmeier, M. (2024). An annotated corpus of tonal piano music from the long 19th century. Empirical Musicology Review, 18(1), 84–95. https://doi.org/10.18061/emr.v18i1.8903

## License

Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License ([CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/)).

![cc-by-nc-sa-image](https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png)

## File naming convention
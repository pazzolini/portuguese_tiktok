# Overview

This repository contains the data collection scripts and configuration files used for the research project, "Mapping the Political Landscape of Portuguese TikTok: From Engagement Patterns to Ideological Classification," developed at INESC TEC.

The primary goal of this project is to quantitatively analyze the activity, thematic focus, and ideological expression of Portuguese political actors on the TikTok platform. In the spirit of open and reproducible science, the tools developed for the data collection phase of this project are made publicly available here.

Please note that while the data collection scripts are shared, the full analysis notebooks and the final datasets are being withheld for future academic publication.

## Repository Contents

- config/: This directory contains the JSON files used to define the list of TikTok accounts for data collection. These files are periodically updated.

- scripts/: This directory contains the Python scripts developed to interact with the TikTok Research API, such as:

  - political_parties_info.py: Collects profile information for party accounts.

  -  political_parties_following.py: Collects the list of accounts followed by parties.

  - political_parties_reposted.py: Collects data on videos reposted by parties.

  - political_personalities_info.py: Collects profile information for individual personality accounts.

  - political_personalities_following.py: Collects the list of accounts followed by personalities.

  - political_personalities_reposted.py: Collects data on videos reposted by personalities.


### How to Cite
If you use the scripts or configuration files from this repository in your research, please cite our upcoming work. A full citation will be added here upon publication.

### Contact
For any questions regarding this project, please contact me at [vitor.r.ferreira@inesctec.pt].

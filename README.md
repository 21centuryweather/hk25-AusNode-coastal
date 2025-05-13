# Diurnal cycle of coastal winds and rainfall  

<img src='https://21centuryweather.org.au/wp-content/uploads/Hackathon-Image-WCRP-Positive-1536x736.jpg' align="centre" height="100" />


### **Project description**

The diurnal (daily) cycle in wind and rainfall is important to understand for weather forecasting in coastal regions and has an impact on the climate system through convection in the tropics. However, numerical models often have biases in the timing, amplitude, and location in the diurnal cycle compared with observations. This project will investigate the diurnal cycle in wind and rainfall in global convection-permitting models, with a focus on the offshore propagation of winds and rainfall related to the sea/land breeze circulation. In addition to providing insight to the physical processes occurring in these high-resolution models, the results may be able to be used in the future to guide the parameterisation of these small-scale processes in coarser models. Our research questions are as follows:

1) What is the global distribution of coastal and offshore diurnal wind variability in different models, including those with and without parameterised convection, and how does this compare with previous theoretical and observational studies?

2) How does the distribution and timing of diurnal wind variability relate to diurnal variability in rainfall in different regions of the globe?

3) Can sea breeze identification methods be applied to global model data, to investigate sea breeze characteristics?

See [Project description](/Project%20description.pdf) for further details.  

### **Project leads:** 

* Andrew Brown, University of Melbourne (@andrewbrown31)

* Bethan White, University of Melbourne (@bethanwhite)

**Project members:** 

* Edward Kyle Badua, Manila Observatory (@kylebadua)

* Huazhen Li, UNSW (@HuazhenLi)

* Anupiya Ellepola, ANU, (@AVEllepola)

<!-- **Collaborators:** list here other collaborators to the project. -->

### **Data:**

**ICON and UM**
* TODO: Insert variable names
* U and V wind components at 10 m
* Rainfall
* Temperature and humidity at 2 m
* Land-sea mask

**Other datasets**
* We may use [ERA5 reanalysis data on NCI](https://dx.doi.org/10.25914/5fb115a9abecf) to compare with. So for project members, please join the [```rt52``` NCI project](https://my.nci.org.au/mancini/project/rt52/join)

## Contributing Guidelines

> The group will decide how to work as a team. This is only an example. 

This section outlines the guidelines to ensure everyone can work and collaborate. All project members have write access to this repository, to avoid overlapping and merge issues make sure you discuss the plan and any changes to existing code or analysis.

### Project organisation

All tasks and activities will be managed through GitHub Issues. While most discussions will take place face-to-face, it is important to document the main ideas and decisions on an issue. Issues will be assigned to one or more people and classified using labels. If you want to work on an issue, comment and make sure is assigned to you to avoid overlapping. If you find a problem in the code or a new task, you can open an issue. 

### How to collaborate

* **Main branch:** We want to keep things simple, if you are working on a notebook alone you can push changes to the main branch. Make sure to 1) only add and commit that file and nothing else, 2) pull from the remote repo and 3) push.

* **Working on a branch:** if you want to fix or propose a change to someone else code you will need to create a branch and open a pull request. Make sure you explain your suggestion in the pull request message. **This also applies to collaborators outside the project team.**

### Repository structure

```bash
hk25-AusNode-coastal/
├── LICENCE
├── README.md
├── Project description.pdf
├── analysis/
│   ├── analysis.py
│   ├── __init__.py
│   ├── read.py
│   └── sea_breeze/
│       ├── load_model_data.py
│       ├── sea_breeze_filters.py
│       ├── sea_breeze_funcs.py
│       └── utils.py
├── tests/
│   ├── test_analysis.py
│   └── test_read.py

```
* `tests/` this folder contains test code that verifies that your code does what it should.
* `analysis/` this folder will include analysis code and notebooks.
* `analysis/sea_breeze/` this folder contains generic functions to perform sea breeze identification (copied from [andrewbrown31/sea_breeze](https://github.com/andrewbrown31/sea_breeze))


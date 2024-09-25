# Source datasets

They are downloaded with the `source_input` job and they are:

- BAG  
- AHN
- TOP10NL ([xsd](https://register.geostandaarden.nl/gmlapplicatieschema/top10nl/1.2.0/top10nl.xsd)|[Code Lists](https://register.geostandaarden.nl/waardelijst/top10nl/20190708))


## BAG

### Definitions

[**BAG (Basisregistratie Adressen en Gebouwen):**](https://www.kadaster.nl/zakelijk/registraties/basisregistraties/bag/over-bag) It contains data on all addresses and buildings in the Netherlands, such as year of construction, surface area, usage and location. The BAG is part of the government system of basic registrations. Municipalities are source holders of the BAG -  they are responsible for collecting it and recording its quality. The BAG dataset is created in accordance with the [Official BAG specifications (BAG Catalogus 2018).](https://www.geobasisregistraties.nl/documenten/publicatie/2018/03/12/catalogus-2018). 

**LVBAG(De Landelijke Voorziening BAG):** Municipalities are responcible for collecting BAG data and them making them centrally available through LVBAG. The Kadaster then manages the LV BAG and makes the data available to various customers. 

[**BAG Extract 2.0**](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract): It is a periodic extract from the LVBAG, created by the Kadaster. It is distributed in various manners; We are using the [free, downloadable version](https://www.kadaster.nl/-/kosteloze-download-bag-2-0-extract), which gets updated every month (on the 8th). Alternatively, there are daily and monthly extracts with mutations, per municipality or for the whole country, which are accessible through a subscription.

**Notes**

Technically, we could keep our BAG database up-to-date by processing monthly mutations, but the mutations are only available through a subscription. Therefore, we need to drop and recreate our BAG tables from the national extract each time we update the data. In fact, this is one of the recommended methods in the [Functioneele beschrijving mutatiebestaanded](https://www.kadaster.nl/-/functionele-beschrijving-mutatiebestanden) documentation: *"Het actualiseren van de lokaal ingerichte database kan door middel van het maandelijks inladen van een volledig BAG 2.0 Extract of door het verwerken van mutatiebestanden."*

We can reconstruct the BAG input at any give time (Ts) by selecting on `begingeldigheid <= Ts <= eindgeldigheid`.

The `oorspronkelijkbouwjaar` is not an indicator of a change in the geometry.

**Some links**

[BAG object history documentation](https://www.kadaster.nl/-/specificatie-bag-historiemodel)

[Official BAG specifications (BAG Catalogus 2018)](https://www.geobasisregistraties.nl/documenten/publicatie/2018/03/12/catalogus-2018)

[BAG-API GitHub repo](https://github.com/lvbag/BAG-API)

[Official BAG viewer](https://bagviewer.kadaster.nl/lvbag/bag-viewer/)

[BAG quality dashboard](https://www.kadaster.nl/zakelijk/registraties/basisregistraties/bag/bag-voor-afnemers/bag-kwaliteitsdashboard-voor-afnemers)

## AHN

The [National Height Model of the Netherlands (AHN)](https://www.ahn.nl/) is the openly available elevation data set of the Netherlands. This is acquired through airborne laser scanning (LiDAR), with an average point density of 8 points per square meter for the current version.

For the 3DBAG we use a smart combination of AHN3 and AHN4. AHN4 was acaquired between 2014 and 2019, and AHN4 between 2020 and 2022. [Here](https://www.ahn.nl/historie) you can find the collection dates for each region in the Netherlands. Be aware that if a building was very recently constructed or changed, it can happen that this building does not exist or is no longer correct in the the 3DBAG.

For the latest versions of the 3DBAG we use both AHN3 and AHN4. This is to guarantee the best possible 3D reconstruction for each building. If a building has no mutation since the acquisition of AHN3, we pick the pointcloud with the best point coverage. This reduces the odds that a building contains small errors due to large no data gaps in the point cloud.

There can always be some variation in the point density between buildings and even within one building. There can be no data gaps in the point cloud, caused by an occlusion through objects, water ponds on roofs and scan angle. The number of available points, their distribution and accurate classification has a very significant impact on the quality of the reconstructed models. The quality attributes that we calculate for and assign to each model provide an indication of this quality.

## TOP10NL

The TOP10NL is part of the [TOPNL data sets](https://www.kadaster.nl/zakelijk/producten/geo-informatie/topnl), which belong to the [Topographic Register](https://www.kadaster.nl/zakelijk/registraties/basisregistraties/brt) of the Netherlands. The TOP10NL can be used at various scales, ranging from 1:5,000 to 1:25,000. It models several object types, including buildings and their function. The TOPNL data can be used as data source, as well as base maps in visualisations.

From the TOP10NL we only use the buildings in order to identify the greenhouses and large warehouses among the BAG objects. Due to their glass roof, greenhouses are problematic for our reconstruction algorithm. Therefore we only model these with a simplified shape.


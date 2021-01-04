<h1> VMware has ended active development of this project, this repository will no longer be updated.</h1><br># Data Loader with Pivotal Cloud Cache

This demo is used to load sample transaction data into GemFire/Pivotal Cloud Cache. 

Sample data format is like below:
```
ssn|first|last|gender|street|city|state|zip|latitude|longitude|city_pop|job|dob|account_num|profile|transaction_num|transaction_date|category|amount
995-94-6437|Brigid|Aufderhar|F|8862 Gulgowski Camp|Middlebranch|OH|44652|40.8951|-81.3262|80|Sub|1982-01-09|33479|female_30_40_smaller_cities.json|14|2013-03-23|shopping_net|131.01
```
Each record will be converted into JSON format and serialized by using PDX serializer before stored into PCC.

## Installing

Connect to your PCC cluster and create a `Transactions` region:

```
gfsh> create region --name=Transactions --type=PARTITION
```

Download this repo and compile. Then deploy this app on your Cloud Foundry workspace using the sample manifest. Make sure app is bound with your PCC cluster.

## Use Demo

Open your browser and target to the app url. You will be able to see a UI as below:

![IMG_002](https://github.com/Pivotal-Field-Engineering/pad-pcc-dataloader/blob/master/images/PCC-DataLoader-UI.png)

Type in the batch number for each PUT operation, and click `start` button to start loading.
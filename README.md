This Azure Function utilizes the Tweepy API to query every single tweet containing the #COVID19 hashtag.
It then stores each tweet's ID, text, and creation date in a .CSV file which is uploaded to Azure Storage.

## Instructions

An **Azure Blob Storage Account** and an **Azure Function App** with a **B1 App Service Plan** will be need to run the function in the cloud.
Alternatively, it can be run completely locally in **VS Code** by installing the **Azure Functions** extension and Azure's **Storage Emulator**.

The following values need to be added to the Application Settings through the Azure Portal (or through a `local.settings.json` file if running locally) :
* **STORAGE_ACCOUNT_NAME** - the name of storage **account** where the data will be stored (**not** the container name)
* **STORAGE_CONNECTION_STRING** - connection string of the storage account
* **CONSUMER_API_KEY** - Twitter API Key
* **CONSUMER_API_SECRET** - Twitter API Secret

Then, three containers need to be created in the storage account, with the names `twitterdatatemp`, `twitterdataraw`, and `twitterfunctionlogs`.

## Functionality

The function can actually query for any specific phrase by changing the variable `QUERY` in the *Options* section.

When triggered, the function will start gathering tweets and logging the progress periodically. It can take up to an hour to gather all tweets, but the function is configured to run for a maximum of 3 hours, which can be changed in the `host.json` file.


Tweets will be periodically appended to the temporary storage file, so that if the function fails, progress is not lost. In case of an error, the function will retry up to 3 times, continuing from the last tweet found.

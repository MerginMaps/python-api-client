# mergin_client.ProjectApi

All URIs are relative to *https://localhost/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_project**](ProjectApi.md#add_project) | **POST** /project | Add a new mergin project.
[**delete_project**](ProjectApi.md#delete_project) | **DELETE** /project/{project_name} | Delete a project.
[**download_project**](ProjectApi.md#download_project) | **GET** /project/download/{project_name} | Download dir for single project.
[**download_project_file**](ProjectApi.md#download_project_file) | **GET** /project/raw/{project_name} | Download single project file.
[**get_project**](ProjectApi.md#get_project) | **GET** /project/{project_name} | Find project by name.
[**get_projects**](ProjectApi.md#get_projects) | **GET** /project | Find all mergin projects.
[**update_project**](ProjectApi.md#update_project) | **POST** /project/{project_name} | Update an existing project.


# **add_project**
> add_project(project)

Add a new mergin project.



### Example
```python
from __future__ import print_function
import time
import mergin_client
from mergin_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basicAuth
configuration = mergin_client.Configuration()
configuration.username = 'YOUR_USERNAME'
configuration.password = 'YOUR_PASSWORD'

# create an instance of the API class
api_instance = mergin_client.ProjectApi(mergin_client.ApiClient(configuration))
project = mergin_client.Project() # Project | Project object that needs to be added to the database.

try:
    # Add a new mergin project.
    api_instance.add_project(project)
except ApiException as e:
    print("Exception when calling ProjectApi->add_project: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | [**Project**](Project.md)| Project object that needs to be added to the database. | 

### Return type

void (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_project**
> delete_project(project_name)

Delete a project.



### Example
```python
from __future__ import print_function
import time
import mergin_client
from mergin_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basicAuth
configuration = mergin_client.Configuration()
configuration.username = 'YOUR_USERNAME'
configuration.password = 'YOUR_PASSWORD'

# create an instance of the API class
api_instance = mergin_client.ProjectApi(mergin_client.ApiClient(configuration))
project_name = 'project_name_example' # str | name of project to delete.

try:
    # Delete a project.
    api_instance.delete_project(project_name)
except ApiException as e:
    print("Exception when calling ProjectApi->delete_project: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_name** | **str**| name of project to delete. | 

### Return type

void (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **download_project**
> download_project(project_name, format=format)

Download dir for single project.

### Example
```python
from __future__ import print_function
import time
import mergin_client
from mergin_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basicAuth
configuration = mergin_client.Configuration()
configuration.username = 'YOUR_USERNAME'
configuration.password = 'YOUR_PASSWORD'

# create an instance of the API class
api_instance = mergin_client.ProjectApi(mergin_client.ApiClient(configuration))
project_name = 'project_name_example' # str | name of project to return.
format = 'format_example' # str | output format. (optional)

try:
    # Download dir for single project.
    api_instance.download_project(project_name, format=format)
except ApiException as e:
    print("Exception when calling ProjectApi->download_project: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_name** | **str**| name of project to return. | 
 **format** | **str**| output format. | [optional] 

### Return type

void (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/zip

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **download_project_file**
> download_project_file(project_name, file)

Download single project file.

### Example
```python
from __future__ import print_function
import time
import mergin_client
from mergin_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basicAuth
configuration = mergin_client.Configuration()
configuration.username = 'YOUR_USERNAME'
configuration.password = 'YOUR_PASSWORD'

# create an instance of the API class
api_instance = mergin_client.ProjectApi(mergin_client.ApiClient(configuration))
project_name = 'project_name_example' # str | name of project.
file = 'file_example' # str | path of file.

try:
    # Download single project file.
    api_instance.download_project_file(project_name, file)
except ApiException as e:
    print("Exception when calling ProjectApi->download_project_file: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_name** | **str**| name of project. | 
 **file** | **str**| path of file. | 

### Return type

void (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/zip

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_project**
> ProjectDetail get_project(project_name)

Find project by name.

Returns a single project.

### Example
```python
from __future__ import print_function
import time
import mergin_client
from mergin_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basicAuth
configuration = mergin_client.Configuration()
configuration.username = 'YOUR_USERNAME'
configuration.password = 'YOUR_PASSWORD'

# create an instance of the API class
api_instance = mergin_client.ProjectApi(mergin_client.ApiClient(configuration))
project_name = 'project_name_example' # str | name of project to return

try:
    # Find project by name.
    api_response = api_instance.get_project(project_name)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ProjectApi->get_project: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_name** | **str**| name of project to return | 

### Return type

[**ProjectDetail**](ProjectDetail.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_projects**
> list[ProjectListItem] get_projects()

Find all mergin projects.

Returns list of all projects.

### Example
```python
from __future__ import print_function
import time
import mergin_client
from mergin_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basicAuth
configuration = mergin_client.Configuration()
configuration.username = 'YOUR_USERNAME'
configuration.password = 'YOUR_PASSWORD'

# create an instance of the API class
api_instance = mergin_client.ProjectApi(mergin_client.ApiClient(configuration))

try:
    # Find all mergin projects.
    api_response = api_instance.get_projects()
    pprint(api_response)
except ApiException as e:
    print("Exception when calling ProjectApi->get_projects: %s\n" % e)
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**list[ProjectListItem]**](ProjectListItem.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_project**
> update_project(project_name, project)

Update an existing project.



### Example
```python
from __future__ import print_function
import time
import mergin_client
from mergin_client.rest import ApiException
from pprint import pprint

# Configure HTTP basic authorization: basicAuth
configuration = mergin_client.Configuration()
configuration.username = 'YOUR_USERNAME'
configuration.password = 'YOUR_PASSWORD'

# create an instance of the API class
api_instance = mergin_client.ProjectApi(mergin_client.ApiClient(configuration))
project_name = 'project_name_example' # str | name of project that need to be updated.
project = mergin_client.Project1() # Project1 | Project object that needs to be updated.

try:
    # Update an existing project.
    api_instance.update_project(project_name, project)
except ApiException as e:
    print("Exception when calling ProjectApi->update_project: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_name** | **str**| name of project that need to be updated. | 
 **project** | [**Project1**](Project1.md)| Project object that needs to be updated. | 

### Return type

void (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


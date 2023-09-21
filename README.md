The Zypp Containers package retrieves the container parameters from that have been configured in your data portal. The package operates under the assumption that you have a `Zypp dataportaal` and use the standard datamodel that comes with it.

The `get_container_params` is the main function in the package. As parameters it accepts the name of the container for which you are trying to retrieve the parameters aswell as the schema under which the container tables can be found in your database. The `schema` parameter has a default value of `dataportaal`. The parameters are returned as a dictionary.

### Example
The below code could be found on the image of the container that you want to run. Originally, the `year` and `category` parameter might have been hardcoded into the script. Now, these can be configured in the database (through your dataportal) and retrieved when the container is started.

```py
from zypp_containers import get_container_params

def run(year, category):
  # .....

parameters = get_container_params('afas_consolidation')
year, category = parameters["year"], parameters["category"]
run_afas_consolidation(year=year, category=category)
```

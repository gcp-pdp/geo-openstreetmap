``from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests


IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'


def trigger_dag(data, context=None):
    """Makes a POST request to the Composer DAG Trigger API
    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.
    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python
    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.
    """

    # Fill in with your Composer info here
    # Navigate to your webserver's login page and get this from the URL
    # Or use the script found at
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/composer/rest/get_client_id.py
    client_id = '1048524459548-144k61tbj4rjha5aqvt2d5ftstihv7da.apps.googleusercontent.com'
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = 'g9b35b5fa26123a34p-tp'
    # The name of the DAG you wish to trigger
    dag_name = 'osm_to_big_query'
    webserver_url = (
        'https://'
        + webserver_id
        + '.appspot.com/api/experimental/dags/'
        + dag_name
        + '/dag_runs'
    )
    # Make a POST request to IAP which then Triggers the DAG
    make_iap_request(
        webserver_url, client_id, method='POST', json={"conf": data})


# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE
def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text
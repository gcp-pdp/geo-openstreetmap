import argparse
import sys
import logging

logging.getLogger().setLevel(logging.INFO)

def get_client_id(project_id, location, composer_environment):
    import google.auth
    import google.auth.transport.requests
    import requests
    import six.moves.urllib.parse

    # Authenticate with Google Cloud.
    # See: https://cloud.google.com/docs/authentication/getting-started
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = google.auth.transport.requests.AuthorizedSession(
        credentials)

    environment_url = (
        'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
        '/environments/{}').format(project_id, location, composer_environment)
    composer_response = authed_session.request('GET', environment_url)
    environment_data = composer_response.json()
    airflow_uri = environment_data['config']['airflowUri']

    # The Composer environment response does not include the IAP client ID.
    # Make a second, unauthenticated HTTP request to the web server to get the
    # redirect URI.
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers['location']

    # Extract the client_id query parameter from the redirect.
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)
    return query_string['client_id'][0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Project ID.')
    parser.add_argument(
        'location', help='Region of the Cloud Composer environent.')
    parser.add_argument(
        'composer_environment', help='Name of the Cloud Composer environent.')

    args = parser.parse_args()
    logging.info(args)
    client_id = get_client_id(args.project_id, args.location, args.composer_environment)
    logging.info(client_id)
    sys.exit(0)

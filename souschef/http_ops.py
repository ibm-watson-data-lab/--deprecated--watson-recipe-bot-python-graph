from base64 import b64encode
import json
import requests
import sys


class HttpOps(object):

    def __init__(self, credentials, logger, version=0):
        self.creds = credentials
        self.headers = {'content-type': 'application/json'}
        #if version == 1:
        #    self.headers = {'content-type': 'application/vnd.ibmgraph-v1.json'}
        auth_str = 'Basic {0}'.format(b64encode('{0}:{1}'.format(
            self.creds['user'],
            self.creds['password']
        )))
        self.headers['authorization'] = auth_str
        self.logger = logger

    def do_get(self, url_path):
        try:
            curl_qry = "curl -X 'GET' -H 'Authorization:{0}' -H 'Content-Type:{1}' {2}".format(
                self.headers['authorization'],
                self.headers['content-type'],
                self.creds['api_url'] + url_path
            )
            self.logger.info(curl_qry)
            resp = requests.get(
                self.creds['api_url'] + url_path,
                headers=self.headers
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                self.logger.warn('Response is: {0}'.format(
                    json.dumps(resp.json())
                ))
                resp.raise_for_status()
        except requests.exceptions.RequestException as exception:
            self.logger.error(exception)
            sys.exit(1)
        self.logger.warn('Returning None from http_ops.do_get')
        return None

    def do_post(self, url_path, data=None, file_path=None):
        """
        Triggers an HTTP POST request for the given urlPath and data
        @param urlPath String location on the server where data is to be posted
        @param data JSON Data to POST
        """
        try:
            curl_qry = None
            resp = None
            if file_path is None and data is not None:
                curl_qry = "curl -X 'POST' -d '{0}' -H 'Authorization:{1}' -H 'Content-Type:{2}' {3}".format(
                    json.dumps(data),
                    self.headers['authorization'],
                    self.headers['content-type'],
                    self.creds['api_url'] + url_path
                )
                self.logger.info(curl_qry)
                resp = requests.post(
                    self.creds['api_url'] + url_path,
                    data=json.dumps(data),
                    headers=self.headers
                )
            elif file_path is not None and data is None:
                curl_qry = "curl -v -X 'POST' -F 'graphson=@{0}' -H '{1}' {2}".format(
                    file_path,
                    json.dumps(self.headers),
                    self.creds['api_url'] + url_path
                )
                self.logger.info(curl_qry)
                files = {'file': open(file_path, 'rb')}
                resp = requests.post(
                    self.creds['api_url'] + url_path,
                    headers=self.headers,
                    files=files
                )
                self.logger.info('****')
                self.logger.info(self.get_curl_cmd(resp))
                self.logger.info('****')
                self.logger.info(
                    'HTTP POST request executed with status {0}'.format(
                        json.dumps(resp.status_code)
                    ))
            if resp.status_code == 200:
                return resp.json()
            else:
                self.logger.warn('Response is: {0}'.format(
                    json.dumps(resp.json())
                ))
                resp.raise_for_status()
        except requests.exceptions.RequestException as exception:
            self.logger.error(exception)
            sys.exit(1)
        self.logger.warn('Returning None from http_ops.do_post')
        return None

    def get_curl_cmd(self, response):
        req = response.request

        command = "curl -X '{method}' -H '{headers}' -d '{data}' '{uri}'"
        method = req.method
        uri = req.url
        data = req.body
        headers = ["{0}: {1}".format(k, v) for k, v in req.headers.items()]
        headers = " -H ".join(headers)
        return command.format(
            method=method, headers=headers, data=data, uri=uri
        )

    def do_delete(self, url_path):
        try:
            curl_qry = "curl -H '{0}' {1}".format(
                json.dumps(self.headers),
                self.creds['api_url'] + url_path
            )
            self.logger.info(curl_qry)
            resp = requests.delete(
                self.creds['api_url'] + url_path,
                headers=self.headers
            )
            self.logger.info(
                'HTTP DELETE request executed with status {0}'.format(
                    json.dumps(resp.status_code)
                )
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                self.logger.warn('Response is: {0}'.format(resp.text))
                resp.raise_for_status()
        except requests.exceptions.RequestException as exception:
            self.logger.error(exception)
            sys.exit(1)
        self.logger.warn('Returning None from http_ops.do_delete')
        return None

    def log_in(self):
        try:
            login_url = self.creds['api_url'].split('/g')[0] + '/_session'
            resp = requests.get(
                login_url,
                headers=self.headers
            )
            self.logger.info(
                'HTTP GET session login request executed with status {0}'.format(
                    json.dumps(resp.status_code)
                )
            )
            if resp.status_code == 200:
                resp = resp.json()
                if type(resp) is str:
                    resp = json.loads(resp)
                if 'gds-token' in resp:
                    gds_token = 'gds-token {0}'.format(resp['gds-token'])
                    self.logger.info('auth token is: {0}'.format(gds_token))
                    self.headers['authorization'] = gds_token
                return gds_token
            else:
                self.logger.warn('Response is {0}'.format(
                    json.dumps(resp.text)
                ))
                resp.raise_for_status()
        except requests.exceptions.RequestException as exception:
            self.logger.error(exception)
            sys.exit(1)
        self.logger.warn('Returning None from http_ops.log_in')
        return None


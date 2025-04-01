# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import Flask, request

from rucio.common.exception import (
    AccessDenied,
    DuplicateLoadInjectionPlan,
)

from rucio.gateway.loadinjection import add_load_injection_plans
from rucio.web.rest.flaskapi.authenticated_bp import AuthenticatedBlueprint
from rucio.web.rest.flaskapi.v1.common import (
    ErrorHandlingMethodView,
    check_accept_header_wrapper_flask,
    generate_http_error_flask,
    json_list,
    response_headers,
)


class BulkPlans(ErrorHandlingMethodView):

    @check_accept_header_wrapper_flask(["application/json"])
    def post(self):
        """
        ---
        summary: Add load injection plans bulk
        description: Add new load injection plans in bulk
        tags:
          - Load Injection Plans
        requestBody:
          content:
            application/json:
              schema:
                type: array
                items:
                  description: One plan to add.
                  type: object
                  required:
                    - src_rse
                    - dest_rse
                    - inject_rate
                    - start_time
                    - end_time
                    - comments
                    - interval
                    - fudge
                    - max_injection
                    - expiration_delay
                    - rule_lifetime
                    - big_first
                    - dry_run
                  properties:
                    src_rse:
                      description: Source RSE name
                      type: string
                    dest_rse:
                      description: Destination RSE name
                      type: string
                    inject_rate:
                      description: Injection rate in MB/s
                      type: integer
                    start_time:
                      description: Start time of the injection plan
                      type: string
                    end_time:
                      description: End time of the injection plan
                      type: string
                    comments:
                      description: Comments for the injection plan
                      type: string
                    interval:
                      description: Time interval between injections in seconds
                      type: integer
                    fudge:
                      description: Fudge factor for the injection plan
                      type: float
                    max_injection:
                      description: Maximum injection rate
                      type: float
                    expiration_delay:
                      description: Expiration delay for the injection plan
                      type: integer
                    rule_lifetime:
                      description: Rule lifetime for the injection plan
                      type: integer
                    big_first:
                      description: Big first flag for the injection plan
                      type: boolean
                    dry_run:
                      description: Dry run flag for the injection plan
                      type: boolean
        responses:
          201:
            description: OK
            content:
              application/json:
                schema:
                  type: string
                  enum: ["Created"]
          401:
            description: Invalid Auth Token
          406:
            description: Not acceptable
          409:
            description: Plan conflicts with existing ones
        """
        plans = json_list()

        try:
            add_load_injection_plans(
                injection_plans=plans,
                issuer=request.environ.get("issuer"),
                vo=request.environ.get("vo"),
            )
        except AccessDenied as error:
            return generate_http_error_flask(401, error)
        except DuplicateLoadInjectionPlan as error:
            return generate_http_error_flask(409, error)
        except Exception as error:
            return generate_http_error_flask(406, error)
        return "Created", 201


def blueprint(with_doc: bool = False) -> AuthenticatedBlueprint:
    bp = AuthenticatedBlueprint("loadinjection", __name__, url_prefix="/loadinjection")

    bulkplans_view = BulkPlans.as_view("bulkplans")
    bp.add_url_rule(
        "",
        view_func=bulkplans_view,
        methods=[
            "post",
        ],
    )

    bp.after_request(response_headers)
    return bp


def make_doc():
    """Only used for sphinx documentation"""
    doc_app = Flask(__name__)
    doc_app.register_blueprint(blueprint(with_doc=True))
    return doc_app

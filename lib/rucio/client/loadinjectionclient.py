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

from typing import TYPE_CHECKING, Any, Optional

from requests.status_codes import codes

from rucio.client.baseclient import BaseClient, choice
from rucio.common.utils import build_url, render_json

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class LoadInjectionClient(BaseClient):
    """LoadInjectionClient class for managing load injection plans"""

    LOADINJECTION_BASEURL = "loadinjection"

    def add_load_injection_plan(
        self,
        src_rse: str,
        dst_rse: str,
        inject_rate: int,
        start_time: str,
        end_time: str,
        comments: Optional[str] = None,
        interval: Optional[int] = None,
        fudge: Optional[float] = None,
        max_injection: Optional[float] = None,
        expiration_delay: Optional[int] = None,
        rule_lifetime: Optional[int] = None,
        big_first: Optional[bool] = None,
        dry_run: Optional[bool] = None,
    ) -> bool:
        """
        Add a load injection plan.

        :param src_rse: The source RSE.
        :param dst_rse: The destination RSE.
        :param inject_rate: The injection rate, in MB/s.
        :param start_time: The start time of the injection plan.
        :param end_time: The end time of the injection plan.
        :param comments: The comments for the injection plan.
        :param interval: The time interval between eche time injection, in seconds.
        :param fudge: The fudge factor for the injection plan.
        :param max_injection: The maximum injection rate.
        :param expiration_delay: The expiration delay for the injection plan.
        :param rule_lifetime: The rule lifetime for the injection plan.
        :param big_first: The big first flag for the injection plan.
        :param dry_run: The dry run flag for the injection plan.

        :returns: True if the load injection plan was added successfully, False otherwise.
        """

        new_plan = {
            "src_rse": src_rse,
            "dst_rse": dst_rse,
            "inject_rate": inject_rate,
            "start_time": start_time,
            "end_time": end_time,
            "comments": comments,
            "interval": interval,
            "fudge": fudge,
            "max_injection": max_injection,
            "expiration_delay": expiration_delay,
            "big_first": big_first,
            "rule_lifetime": rule_lifetime,
            "dry_run": dry_run,
        }
        return self.add_load_injection_plans([new_plan])

    def add_load_injection_plans(self, plans: "Sequence[Mapping[str, Any]]") -> bool:
        """
        Add load injection plans.

        :param plans: A list of dictionaries containing the load injection plans.

        :returns: True if the load injection plans were added successfully, False otherwise.
        """
        path = "/".join([self.LOADINJECTION_BASEURL])
        url = build_url(choice(self.list_hosts), path=path)
        r = self._send_request(url, type_="POST", data=render_json(plans))
        if r.status_code == codes.created:
            return True
        else:
            exc_cls, exc_msg = self._get_exception(
                headers=r.headers, status_code=r.status_code, data=r.content
            )
            raise exc_cls(exc_msg)

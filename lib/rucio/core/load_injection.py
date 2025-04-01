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

import datetime
import logging
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy.sql.expression import select, and_, not_, exists, delete, update
from sqlalchemy.exc import IntegrityError

from rucio.common import exception
from rucio.db.sqla import models
from rucio.db.sqla.session import read_session, transactional_session

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from sqlalchemy.orm import Session


@read_session
def scan_unique_rse_pair_datasets(
    src_rse_id: str, dest_rse_id: str, *, session: "Session"
) -> "Sequence[Mapping[str, Any]]":
    """
    Scan the unique datasets for a given RSE pair from dataset_locks table.

    :param src_rse_id: The src RSE id.
    :param des_rse_id: The des RSE id.
    :param session: The database session in use.
    :returns: A list of unique datasets for the given RSE pair.
    """

    # Query the dataset_locks table for unique datasets for the given RSE pair
    try:
        stmt = select(models.DatasetLock).where(
            and_(
                models.DatasetLock.state == "O",
                models.DatasetLock.bytes > 0,
                models.DatasetLock.length.between(1, 1000),
                models.DatasetLock.bytes / models.DatasetLock.length > 100000000,
                models.DatasetLock.rse_id == src_rse_id,
                not_(
                    exists().where(
                        and_(
                            models.DatasetLock.scope == models.DatasetLock.scope,
                            models.DatasetLock.name == models.DatasetLock.name,
                            models.DatasetLock.rse_id == dest_rse_id,
                        )
                    )
                ),
            )
        )
        query_result = session.execute(stmt).scalars().all()
    except IntegrityError as error:
        raise exception.RucioException(error.args)

    # Convert the query result to a list of dictionaries with the required format
    result = list()
    for dataset in query_result:
        result.append(
            {
                "scope": dataset.scope,
                "name": dataset.name,
                "bytes": dataset.bytes,
                "length": dataset.length,
                "src_rse_id": src_rse_id,
                "dest_rse_id": dest_rse_id,
            }
        )
    return result


@read_session
def get_unique_rse_pair_datasets(
    src_rse_id: str, dest_rse_id: str, *, session: "Session"
) -> "Sequence[Mapping[str, Any]]":
    """
    Read the cached unique datasets for a given RSE pair from unique_datasets table.

    :param src_rse_id: The src RSE id.
    :param dest_rse_id: The dest RSE id.
    :param session: The database session in use.
    :returns: A list of unique datasets for the given RSE pair.
    """

    try:
        stmt = select(models.LoadInjectionDatasets).where(
            and_(
                models.LoadInjectionDatasets.src_rse_id == src_rse_id,
                models.LoadInjectionDatasets.dest_rse_id == dest_rse_id,
            )
        )
        query_result = session.execute(stmt).scalars().all()
    except IntegrityError as error:
        raise exception.RucioException(error.args)
    return [dataset.to_dict() for dataset in query_result]


@transactional_session
def add_unique_rse_pair_dataset(
    src_rse_id: str,
    dest_rse_id: str,
    scope: str,
    name: str,
    bytes: int,
    length: int,
    *,
    session: "Session"
) -> None:
    """
    Add a unique dataset for a given RSE pair to unique_datasets table.

    :param src_rse_id: The src RSE id.
    :param dest_rse_id: The dest RSE id.
    :param scope: The dataset scope.
    :param name: The dataset name.
    :param bytes: The dataset size.
    :param length: The dataset length.
    :param session: The database session in use.
    """
    add_unique_rse_pair_datasets(
        [
            {
                "scope": scope,
                "name": name,
                "bytes": bytes,
                "length": length,
                "src_rse_id": src_rse_id,
                "dest_rse_id": dest_rse_id,
            }
        ],
        session=session,
    )


@transactional_session
def add_unique_rse_pair_datasets(
    datasets: "Sequence[Mapping[str, Any]]", *, session: "Session"
) -> None:
    """
    Add a list of unique datasets for a given RSE pair to unique_datasets table.

    :param datasets: The list of unique datasets for the given RSE pair.
    :param session: The database session in use.
    """
    try:
        for dataset in datasets:
            new_dataset = models.LoadInjectionDatasets(
                scope=dataset["scope"],
                name=dataset["name"],
                src_rse_id=dataset["src_rse_id"],
                dest_rse_id=dataset["dest_rse_id"],
                bytes=dataset["bytes"],
                length=dataset["length"],
            )
            new_dataset.save(session=session, flush=False)
        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@transactional_session
def delete_unique_rse_pair_dataset(
    src_rse_id: str, dest_rse_id: str, scope: str, name: str, *, session: "Session"
) -> None:
    """
    Delete a unique dataset for a given RSE pair from unique_datasets table.

    :param src_rse_id: The src RSE id.
    :param dest_rse_id: The dest RSE id.
    :param scope: The dataset scope.
    :param name: The dataset name.
    :param session: The database session in use.
    """
    delete_unique_rse_pair_datasets(
        [
            {
                "scope": scope,
                "name": name,
                "src_rse_id": src_rse_id,
                "dest_rse_id": dest_rse_id,
            }
        ],
        session=session,
    )


@transactional_session
def delete_unique_rse_pair_datasets(
    datasets: "Sequence[Mapping[str, Any]]", *, session: "Session"
) -> None:
    """
    Delete a list of unique datasets for a given RSE pair from unique_datasets table.

    :param datasets: The list of unique datasets for the given RSE pair.
    :param session: The database session in use.
    """
    try:
        for dataset in datasets:
            stmt = delete(models.LoadInjectionDatasets).where(
                and_(
                    models.LoadInjectionDatasets.src_rse_id == dataset["src_rse_id"],
                    models.LoadInjectionDatasets.dest_rse_id == dataset["dest_rse_id"],
                    models.LoadInjectionDatasets.scope == dataset["scope"],
                    models.LoadInjectionDatasets.name == dataset["name"],
                )
            )
            session.execute(stmt)
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@read_session
def validate_unique_rse_pair_dataset(
    scope: str, name: str, src_rse_id: str, dest_rse_id: str, *, session: "Session"
) -> bool:
    """
    Varify that the unique rse pair dataset exactly exist in the src and does NOT exist in the dest.

    :param scope: The dataset scope.
    :param name: The dataset name.
    :param src_rse_id: The src RSE id.
    :param dest_rse_id: The dest RSE id.
    :param session: The database session in use.
    :returns: True if the dataset is unique, False otherwise.
    """
    try:
        stmt = select(models.DatasetLock).where(
            and_(
                models.DatasetLock.scope == scope,
                models.DatasetLock.name == name,
                models.DatasetLock.rse_id == src_rse_id,
                not_(
                    exists().where(
                        and_(
                            models.DatasetLock.scope == scope,
                            models.DatasetLock.name == name,
                            models.DatasetLock.rse_id == dest_rse_id,
                        )
                    )
                ),
            )
        )
        query_result = session.execute(stmt).scalars().all()
    except IntegrityError as error:
        raise exception.RucioException(error.args)
    if query_result:
        return True
    else:
        return False


@read_session
def get_injection_plans(
    state: "Optional[str]" = None, *, session: "Session"
) -> "Sequence[Mapping[str, Any]]":
    """
    Get injection plans from the database.

    :param session: The database session in use.
    :returns: A list of injection plans.
    """
    try:
        stmt = select(models.LoadInjectionPlans)
        query_result = session.execute(stmt).scalars().all()
    except IntegrityError as error:
        raise exception.RucioException(error.args)
    if state:
        return [plan.to_dict() for plan in query_result if plan.state == state]
    else:
        return [plan.to_dict() for plan in query_result]


@transactional_session
def add_injection_plan(
    plan_id: str,
    src_rse_id: str,
    dest_rse_id: str,
    inject_rate: int,
    interval: int,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    fudge: float = 0.0,
    max_injection: float = 0.2,
    expiration_delay: int = 1800,
    big_first: bool = False,
    rule_lifetime: int = 3600,
    comments: Optional[str] = None,
    dry_run: bool = False,
    state: Optional["Mapping[str, Any]"] = None,
    *,
    session: "Session"
) -> None:
    """
    Add an injection plan in the database.

    :param plan_id: The plan id.
    :param src_rse_id: The src RSE id.
    :param dest_rse_id: The dest RSE id.
    :param inject_rate: The injection rate.
    :param interval: The time interval between eche time injection.
    :param start_time: The start time of the injection plan.
    :param end_time: The end time of the injection plan.
    :param fudge: The fudge factor for the injection plan.
    :param max_injection: The maximum injection rate.
    :param expiration_delay: The expiration delay for the injection plan.
    :param big_first: The big first flag for the injection plan.
    :param rule_lifetime: The rule lifetime for the injection plan.
    :param comments: The comments for the injection plan.
    :param dry_run: The dry_run flag for the injection plan.
    :param state: The state of the injection plan.
    :param session: The database session in use.
    """
    add_injection_plans(
        [
            {
                "plan_id": plan_id,
                "src_rse_id": src_rse_id,
                "dest_rse_id": dest_rse_id,
                "inject_rate": inject_rate,
                "state": state,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
                "fudge": fudge,
                "max_injection": max_injection,
                "expiration_delay": expiration_delay,
                "big_first": big_first,
                "rule_lifetime": rule_lifetime,
                "comments": comments,
                "dry_run": dry_run,
            }
        ],
        session=session,
    )


@transactional_session
def add_injection_plans(
    injection_plans: "Sequence[Mapping[str, Any]]", *, session: "Session"
) -> None:
    """
    Bulk add injection plans in the database.

    :param injection_plans: The list of injection plans to add.
    :param session: The database session in use.
    """
    try:
        for plan in injection_plans:
            new_plan = models.LoadInjectionPlans(
                plan_id=plan["plan_id"],
                dest_rse_id=plan["dest_rse_id"],
                src_rse_id=plan["src_rse_id"],
                inject_rate=plan["inject_rate"],
                state=plan["state"],
                interval=plan["interval"],
                start_time=plan["start_time"],
                end_time=plan["end_time"],
                fudge=plan["fudge"],
                max_injection=plan["max_injection"],
                expiration_delay=plan["expiration_delay"],
                big_first=plan["big_first"],
                rule_lifetime=plan["rule_lifetime"],
                comments=plan["comments"],
                dry_run=plan["dry_run"],
            )
            new_plan.save(session=session, flush=False)
        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@transactional_session
def add_injection_plan_history(
    plan_id: str,
    src_rse_id: str,
    dest_rse_id: str,
    inject_rate: int,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    comments: Optional[str] = None,
    interval: int = 900,
    fudge: float = 0.0,
    max_injection: float = 0.2,
    expiration_delay: int = 1800,
    rule_lifetime: int = 3600,
    big_first: bool = False,
    dry_run: bool = False,
    state: Optional["Mapping[str, Any]"] = None,
    *,
    session: "Session"
) -> None:
    """
    Add a history injection plan in the database.

    :param src_rse_id: The src RSE id.
    :param dest_rse_id: The dest RSE id.
    :param inject_rate: The injection rate.
    :param interval: The time interval between eche time injection.
    :param start_time: The start time of the injection plan.
    :param end_time: The end time of the injection plan.
    :param fudge: The fudge factor for the injection plan.
    :param max_injection: The maximum injection rate.
    :param expiration_delay: The expiration delay for the injection plan.
    :param big_first: The big first flag for the injection plan.
    :param rule_lifetime: The rule lifetime for the injection plan.
    :param comments: The comments for the injection plan.
    :param dry_run: The dry_run flag for the injection plan.
    :param session: The database session in use.
    """
    add_injection_plans_history(
        [
            {
                "plan_id": plan_id,
                "src_rse_id": src_rse_id,
                "dest_rse_id": dest_rse_id,
                "inject_rate": inject_rate,
                "state": state,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
                "fudge": fudge,
                "max_injection": max_injection,
                "expiration_delay": expiration_delay,
                "big_first": big_first,
                "rule_lifetime": rule_lifetime,
                "comments": comments,
                "dry_run": dry_run,
            }
        ],
        session=session,
    )


@transactional_session
def add_injection_plans_history(
    injection_plans: "Sequence[Mapping[str, Any]]", *, session: "Session"
) -> None:
    """
    Bulk add history injection plans in the database.

    :param injection_plans: The list of injection plans to add.
    :param session: The database session in use.
    """
    try:
        for plan in injection_plans:
            new_plan = models.LoadInjectionPlansHistory(
                dest_rse_id=plan["dest_rse_id"],
                src_rse_id=plan["src_rse_id"],
                inject_rate=plan["inject_rate"],
                state=plan["state"],
                interval=plan["interval"],
                start_time=plan["start_time"],
                end_time=plan["end_time"],
                fudge=plan["fudge"],
                max_injection=plan["max_injection"],
                expiration_delay=plan["expiration_delay"],
                big_first=plan["big_first"],
                rule_lifetime=plan["rule_lifetime"],
                comments=plan["comments"],
                dry_run=plan["dry_run"],
            )
            new_plan.save(session=session, flush=False)
        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@transactional_session
def delete_injection_plan(
    src_rse_id: str, dest_rse_id: str, *, session: "Session"
) -> None:
    """
    Delete an injection plan from the database.

    :param src_rse_id: The source RSE ID.
    :param dest_rse_id: The destination RSE ID.
    :param session: The database session in use.
    """
    delete_injection_plans(
        [
            {
                "src_rse_id": src_rse_id,
                "dest_rse_id": dest_rse_id,
            }
        ],
        session=session,
    )


@transactional_session
def delete_injection_plans(
    injection_plans: "Sequence[Mapping[str, Any]]", *, session: "Session"
) -> None:
    """
    Bulk delete injection plans from the database.

    :param injection_plans: The list of injection plans to delete.
    :param session: The database session in use.
    """
    try:
        for plan in injection_plans:
            stmt = delete(models.LoadInjectionPlans).where(
                models.LoadInjectionPlans.dest_rse_id == plan["dest_rse_id"],
                models.LoadInjectionPlans.src_rse_id == plan["src_rse_id"],
            )
            session.execute(stmt)
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@transactional_session
def update_injection_plan_state(
    src_rse_id: str, dest_rse_id: str, new_state: str, *, session: "Session"
) -> None:
    """
    Update the state of an injection plan.

    :param src_rse_id: The source RSE ID.
    :param dest_rse_id: The destination RSE ID.
    :param new_state: The new state of the injection plan.
    :param session: The database session in use.
    """
    try:
        stmt = (
            update(models.LoadInjectionPlans)
            .where(
                and_(
                    models.LoadInjectionPlans.dest_rse_id == dest_rse_id,
                    models.LoadInjectionPlans.src_rse_id == src_rse_id,
                )
            )
            .values(state=new_state)
        )
        session.execute(stmt)
    except IntegrityError as error:
        raise exception.RucioException(error.args)


@read_session
def get_injection_plan_state(
    src_rse_id: str, dest_rse_id: str, *, session: "Session"
) -> "Optional[str]":
    """
    Get the state of an injection plan.

    :param src_rse_id: The source RSE ID.
    :param dest_rse_id: The destination RSE ID.
    :param session: The database session in use.
    :returns: The state of the injection plan.
    """
    try:
        stmt = select(models.LoadInjectionPlans).where(
            and_(
                models.LoadInjectionPlans.dest_rse_id == dest_rse_id,
                models.LoadInjectionPlans.src_rse_id == src_rse_id,
            )
        )
        query_result = session.execute(stmt).scalar_one_or_none()
    except IntegrityError as error:
        raise exception.RucioException(error.args)

    return query_result.to_dict()["state"] if query_result else None

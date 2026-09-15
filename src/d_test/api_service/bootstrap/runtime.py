"""Resources exposed by the API lifespan to request dependencies."""

from dataclasses import dataclass

from d_test.agent_service.application.management import ManagementService
from d_test.agent_service.runtime.management import ManagementRuntime

from ..auth.providers import IdentityProvider


@dataclass
class ApiRuntime:
    resources: ManagementRuntime
    identity: IdentityProvider

    @property
    def service(self) -> ManagementService:
        return self.resources.service

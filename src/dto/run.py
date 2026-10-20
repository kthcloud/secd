import uuid
import datetime
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional
from app.src.util.setup import get_settings

@dataclass
class Run:
    # auto-generated core fields
    run_id:    str                                  = field(default_factory=lambda: str(uuid.uuid4()).replace('-', ''))
    date:      str                                  = field(init=False)
    namespace: str                                  = field(init=False)
    repo_path: str                                  = field(init=False)
    output_path: str                                = field(init=False)
    pvc_repo_path: str                              = field(default_factory=lambda: get_settings()["k8s"]["pvcPath"])
    pvc_name_output: str                            = field(init=False)
    pv_name_output: str                             = field(init=False)

    # optional overrides / metadata
    metadata:        Any                            = None
    keycloak_user_id: Any                           = None
    image_name:      Optional[str]                  = None
    run_for:         Optional[str]                  = None
    namespace_labels: Any                           = None
    database_name:   Optional[str]                  = None
    database_type:   Optional[str]                  = None
    env_vars:        Optional[Dict[str, str]]       = None
    pvc_name:        Optional[str]                  = None
    vault_role_name: Optional[str]                  = None
    service_name:    Optional[str]                  = None

    def __post_init__(self):
        self.date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.namespace        = f"secd-{self.run_id}"
        base_repo             = get_settings()["path"]["repoPath"]
        self.repo_path        = f"{base_repo}/{self.run_id}"
        self.output_path      = f"{self.repo_path}/outputs/{self.date}-{self.run_id}"
        self.pvc_name_output  = f"secd-pvc-{self.run_id}-output"
        self.pv_name_output   = f"secd-pv-{self.run_id}-output"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict (e.g. for JSON)."""
        return asdict(self)
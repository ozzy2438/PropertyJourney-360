"""Explainable, precision-first customer identity resolution."""

from propertyjourney360.identity.evaluation import (
    ClusterMetrics,
    IdentityMetrics,
    evaluate_clusters,
    evaluate_matches,
)
from propertyjourney360.identity.matcher import (
    ClusterConflict,
    ClusterResolution,
    IdentityResolver,
    ResolverConfig,
)

__all__ = [
    "ClusterConflict",
    "ClusterMetrics",
    "ClusterResolution",
    "IdentityMetrics",
    "IdentityResolver",
    "ResolverConfig",
    "evaluate_clusters",
    "evaluate_matches",
]

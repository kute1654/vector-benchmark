# -*- coding: utf-8 -*-
"""Shared EPS export helpers for the benchmark chart scripts.

All public charts are written as EPS (vector output for LaTeX).  Some chart
scripts compose four panel images into a single overview figure; for those we
also keep a raster copy of every panel under ``results/charts/.png_cache/`` so
the composer never has to parse EPS.  Only the EPS files are user-facing.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

CACHE_DIRNAME = ".png_cache"


def eps_path(path: str | os.PathLike) -> str:
    """Return *path* with an .eps suffix (other suffixes are replaced)."""
    return str(Path(path).with_suffix(".eps"))


def _cache_png(eps: str) -> Path:
    eps_p = Path(eps)
    return eps_p.parent / CACHE_DIRNAME / (eps_p.stem + ".png")


def _compress_eps(path: str) -> None:
    """Optionally shrink raster-heavy EPS with Ghostscript (JPEG for images).

    Overview figures embed PNG panels, so an uncompressed EPS can reach tens of
    megabytes.  When ``gs`` is available we distil it to a much smaller file;
    vector panels are left untouched by the caller.
    """
    gs = shutil.which("gs")
    if not gs:
        return
    tmp = path + ".gs.tmp"
    cmd = [gs, "-q", "-dSAFER", "-dBATCH", "-dNOPAUSE",
           "-sDEVICE=eps2write", "-dEPSCrop",
           "-dAutoFilterColorImages=true", "-dColorImageFilter=/DCTEncode",
           "-dJPEGQ=88", "-sOutputFile=" + tmp, path]
    env = dict(os.environ)
    try:
        subprocess.run(cmd, check=True, env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        # Some environments inject an incompatible LD_LIBRARY_PATH.
        env.pop("LD_LIBRARY_PATH", None)
        try:
            subprocess.run(cmd, check=True, env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            Path(tmp).unlink(missing_ok=True)
            return
    if Path(tmp).exists() and Path(tmp).stat().st_size > 0:
        os.replace(tmp, path)


def save_chart(fig, path: str | os.PathLike, *, raster_cache: bool = True,
               dpi: int = 170, compress: bool = False, **kwargs) -> str:
    """Save *fig* as EPS and optionally keep a PNG copy for panel composition.

    ``kwargs`` are forwarded to ``Figure.savefig`` (e.g. bbox_inches, pad_inches).
    ``compress`` asks Ghostscript to re-distil the EPS (useful for overview
    figures that embed raster panels).
    """
    eps = eps_path(path)
    Path(eps).parent.mkdir(parents=True, exist_ok=True)
    import matplotlib
    # Embed TrueType fonts (instead of Type-3) so EPS is accepted by LaTeX.
    matplotlib.rcParams["ps.fonttype"] = 42
    matplotlib.rcParams["ps.useafm"] = False
    kwargs.setdefault("facecolor", "white")
    fig.savefig(eps, format="eps", **kwargs)
    if compress:
        _compress_eps(eps)
    if raster_cache:
        cache = _cache_png(eps)
        cache.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(cache, dpi=dpi, **kwargs)
    return eps


def panel_png(path: str | os.PathLike) -> str | None:
    """Locate the raster copy used to compose a panel into an overview figure.

    Preference order: the .png_cache copy written by ``save_chart``, then a
    legacy sibling ``.png`` next to the EPS.  ``None`` means the panel has not
    been generated yet.
    """
    p = Path(path)
    cache = _cache_png(str(p.with_suffix(".eps")))
    if cache.exists():
        return str(cache)
    sibling = p.with_suffix(".png")
    if sibling.exists():
        return str(sibling)
    return None


def trim_white(im, threshold: int = 250, pad: int = 2):
    """Trim uniform white margins around a panel image.

    Used by the 4-in-1 overview figures so the stacked panels sit flush against
    each other instead of keeping the per-panel savefig padding.
    """
    import numpy as np
    a = np.asarray(im.convert("RGB"))
    mask = np.any(a < threshold, axis=2)
    if not mask.any():
        return im
    rows = np.where(mask.any(axis=1))[0]
    cols = np.where(mask.any(axis=0))[0]
    box = (max(0, int(cols[0]) - pad), max(0, int(rows[0]) - pad),
           min(im.width, int(cols[-1]) + 1 + pad),
           min(im.height, int(rows[-1]) + 1 + pad))
    return im.crop(box)

import requests

def fetch_obis_data(lat: float, lon: float) -> dict:
    """
    Hämtar data från OBIS API för koordinaterna x och y.

    Args:
        lon (float): Longitud
        lat (float): Latitud

    Returns:
        dict: JSON-svar från API:et
    """
    url = "https://api.obis.org/xylookup"
    params = {"x": lon, "y": lat}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Kastar fel om statuskod ≠ 200
        return response.json()[0]
    except requests.exceptions.RequestException as e:
        return {}


def get_odis_depth(lat: float, lon: float) -> float | None:
    info = fetch_obis_data(lat, lon)
    return info.get("grids", {}).get("bathymetry")


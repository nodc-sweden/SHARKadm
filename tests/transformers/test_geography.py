import pytest

from sharkadm.utils import geography

""" TEST RT90 """


def test_rt90_to_wgs84_with_float():
    """Testa konvertering med numeriska koordinater."""
    northing = 6358256
    easting = 1534939

    lat, lon = geography.rt90_to_wgs84(northing, easting)

    assert isinstance(lat, float)
    assert isinstance(lon, float)

    # Kontrollera att koordinaterna ligger i Sverige
    assert 55 < lat < 70
    assert 10 < lon < 25


def test_rt90_to_wgs84_with_string():
    """Testa konvertering med strängar."""
    lat_float, lon_float = geography.rt90_to_wgs84(6358256, 1534939)
    lat_str, lon_str = geography.rt90_to_wgs84("6358256", "1534939")

    assert lat_str == pytest.approx(lat_float)
    assert lon_str == pytest.approx(lon_float)


def test_rt90_to_wgs84_mallifica():
    """Testa konvertering samma som malifica."""
    lat, lon = geography.rt90_to_wgs84(6358256, 1534939)
    assert lat == pytest.approx(57.348641)
    assert lon == pytest.approx(16.385573)


def test_rt90_to_wgs84_returns_lat_lon():
    """Kontrollera att latitud och longitud returneras i rätt ordning."""
    lat, lon = geography.rt90_to_wgs84(6358256, 1534939)

    assert 55 < lat < 70
    assert 10 < lon < 25


@pytest.mark.parametrize(
    "northing,easting",
    [
        ("invalid", "1534939"),
        ("6358256", "invalid"),
    ],
)
def test_rt90_to_wgs84_invalid_input(northing, easting):
    """Ogiltiga koordinater ska ge ValueError."""
    with pytest.raises(ValueError):
        geography.rt90_to_wgs84(northing, easting)


""" TEST swerf99tm """


def test_sweref99tm_to_wgs84_with_float():
    """Testa konvertering med numeriska koordinater."""
    northing = 6357045
    easting = 583379

    lat, lon = geography.sweref99tm_to_wgs84(northing, easting)

    assert isinstance(lat, float)
    assert isinstance(lon, float)

    # Kontrollera att koordinaterna ligger i Sverige
    assert 55 < lat < 70
    assert 10 < lon < 25


def test_sweref99tm_to_wgs84_with_string():
    """Testa konvertering med strängar."""
    lat_float, lon_float = geography.sweref99tm_to_wgs84(6357045, 583379)
    lat_str, lon_str = geography.sweref99tm_to_wgs84("6357045", "583379")

    assert lat_str == pytest.approx(lat_float)
    assert lon_str == pytest.approx(lon_float)


def test_sweref99tm_to_wgs84_mallifica():
    """Testa konvertering samma som malifica."""
    lat, lon = geography.sweref99tm_to_wgs84(6357045, 583379)
    assert lat == pytest.approx(57.348641)
    assert lon == pytest.approx(16.385573)


def test_sweref99tm_to_wgs84_returns_lat_lon():
    """Kontrollera att latitud och longitud returneras i rätt ordning."""
    lat, lon = geography.sweref99tm_to_wgs84(6357045, 583379)

    assert 55 < lat < 70
    assert 10 < lon < 25


@pytest.mark.parametrize(
    "northing,easting",
    [
        ("invalid", "583379"),
        ("6357045", "invalid"),
    ],
)
def test_sweref99tm_to_wgs84_invalid_input(northing, easting):
    """Ogiltiga koordinater ska ge ValueError."""
    with pytest.raises(ValueError):
        geography.sweref99tm_to_wgs84(northing, easting)


""" decdeg <--> decmin"""


class TestDecdegToDecmin:
    @pytest.mark.parametrize(
        "pos, expected",
        [
            (59.5, "5930.0"),
            (59.25, "5915.0"),
            (12.75, "1245.0"),
            (-59.5, "-5930.0"),
            (-12.75, "-1245.0"),
            (0.5, "030.0"),
            (0.0, "00.0"),
        ],
    )
    def test_conversion(self, pos, expected):
        assert geography.decdeg_to_decmin(pos) == expected

    def test_string_input(self):
        assert geography.decdeg_to_decmin("59.5") == "5930.0"

    def test_with_space(self):
        assert geography.decdeg_to_decmin(59.5, with_space=True) == "59 30.0"
        assert geography.decdeg_to_decmin(-59.5, with_space=True) == "-59 30.0"

    def test_rounding(self):
        assert geography.decdeg_to_decmin(59.123456, nr_decimals=2) == "5907.41"

    def test_rounding_carry(self):
        assert geography.decdeg_to_decmin(12.99999, nr_decimals=2) == "1300.00"

    def test_zero_decimal_places(self):
        assert geography.decdeg_to_decmin(59.5, nr_decimals=0) == "5930"


class TestDecminToDecdeg:
    @pytest.mark.parametrize(
        "pos, expected",
        [
            ("5930.0", 59.5),
            ("5915.0", 59.25),
            ("1245.0", 12.75),
            ("-5930.0", -59.5),
            ("-1245.0", -12.75),
            ("0030.0", 0.5),
            ("0", 0.0),
        ],
    )
    def test_conversion(self, pos, expected):
        assert float(geography.decmin_to_decdeg(pos)) == pytest.approx(expected)

    def test_string_with_space(self):
        assert float(geography.decmin_to_decdeg("59 30.0")) == pytest.approx(59.5)

    def test_negative_with_space(self):
        assert float(geography.decmin_to_decdeg("-59 30.0")) == pytest.approx(-59.5)

    def test_float_input(self):
        assert float(geography.decmin_to_decdeg(5930.0)) == pytest.approx(59.5)

    def test_rounding(self):
        result = geography.decmin_to_decdeg("5930.0", nr_decimals=4)
        assert result == "59.5"

    @pytest.mark.parametrize(
        "pos",
        [
            "5960.0",
            "5999.0",
            "-5960.0",
        ],
    )
    def test_invalid_minutes(self, pos):
        with pytest.raises(ValueError):
            geography.decmin_to_decdeg(pos)


class TestCoordinateRoundtrip:
    @pytest.mark.parametrize(
        "coordinate",
        [
            59.5,
            59.123456,
            12.75,
            -59.5,
            -12.75,
            0.5,
            0.0,
        ],
    )
    def test_roundtrip(self, coordinate):
        decmin = geography.decdeg_to_decmin(coordinate, nr_decimals=8)
        result = float(geography.decmin_to_decdeg(decmin))

        assert result == pytest.approx(coordinate, abs=1e-8)

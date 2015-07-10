import re

from .base import GeocodeService
import json
import logging
from omgeo.places import Candidate
from omgeo.preprocessors import CancelIfPOBox, CountryPreProcessor, RequireCountry, \
    ParseSingleLine, ReplaceRangeWithNumber
from omgeo.postprocessors import AttrFilter, AttrExclude, AttrRename, AttrSorter, \
    AttrMigrator, UseHighScoreIfAtLeast, GroupBy, ScoreSorter
import time
try:
    from urllib import unquote
except ImportError:
    from urllib.parse import unquote
logger = logging.getLogger(__name__)


class Bing(GeocodeService):
    """
    Class to geocode using Bing services:
     * `Find a Location by Query <http://msdn.microsoft.com/en-us/library/ff701711.aspx>`_
     * `Find a Location by Address <http://msdn.microsoft.com/en-us/library/ff701714.aspx>`_

    Settings used by the Bing GeocodeService object may include:
     * api_key --  The API key used to access Bing services.

    """
    _endpoint = 'http://dev.virtualearth.net/REST/v1/Locations'

    DEFAULT_PREPROCESSORS = [
        ReplaceRangeWithNumber()
    ]

    DEFAULT_POSTPROCESSORS = [
       AttrMigrator('confidence', 'score',
                    {'High':100, 'Medium':85, 'Low':50}),
       UseHighScoreIfAtLeast(100),
       AttrFilter(['Address', 'AdministrativeBuilding',
                   'AgriculturalStructure',
                   'BusinessName', 'BusinessStructure',
                   'BusStation', 'Camp', 'Church', 'CityHall',
                   'CommunityCenter', 'ConventionCenter',
                   'Courthouse', 'Factory', 'FerryTerminal',
                   'FishHatchery', 'Fort', 'Garden', 'Geyser',
                   'Heliport', 'IndustrialStructure',
                   'InformationCenter', 'Junction',
                   'LandmarkBuilding', 'Library', 'Lighthouse',
                   'Marina', 'MedicalStructure', 'MetroStation',
                   'Mine', 'Mission', 'Monument', 'Mosque',
                   'Museum', 'NauticalStructure', 'NavigationalStructure',
                   'OfficeBuilding', 'ParkAndRide', 'PlayingField',
                   'PoliceStation', 'PostOffice', 'PowerStation',
                   'Prison', 'RaceTrack', 'ReligiousStructure',
                   'RestArea', 'Ruin', 'ShoppingCenter', 'Site',
                   'SkiArea', 'Spring', 'Stadium', 'Temple',
                   'TouristStructure'], 'entity'),
       AttrRename('locator', dict(Rooftop='rooftop',
                                  Parcel='parcel',
                                  ParcelCentroid='parcel',
                                  Interpolation='interpolation',
                                  InterpolationOffset='interpolation_offset')),
       AttrSorter(['rooftop', 'parcel',
                   'interpolation_offset', 'interpolation'],
                   'locator'),
       AttrSorter(['Address'], 'entity'),
       ScoreSorter(),
       GroupBy(('x', 'y')),
       GroupBy('match_addr')]
    DEFAULT_POSTPROCESSORS = []

    def __init__(self, preprocessors=None, postprocessors=None, settings=None):
        preprocessors = Bing.DEFAULT_PREPROCESSORS if preprocessors is None else preprocessors
        postprocessors = Bing.DEFAULT_POSTPROCESSORS if postprocessors is None else postprocessors
        GeocodeService.__init__(self, preprocessors, postprocessors, settings)

    def _geocode(self, pq):
        if pq.query.strip() == '':
            # No single line query string; use address elements:
            query = {'addressLine': pq.address,
                     'locality': pq.city,
                     'adminDistrict': pq.state,
                     'postalCode': pq.postal,
                     'countryRegion': pq.country}
        else:
            query = {'query': pq.query}

        if pq.viewbox is not None:
            query = dict(query, **{'umv':pq.viewbox.to_bing_str()})
        if hasattr(pq, 'culture'):
            query = dict(query, c=pq.culture)
        if hasattr(pq, 'user_ip'):
            query = dict(query, uip=pq.user_ip)
        if hasattr(pq, 'user_lat') and hasattr(pq, 'user_lon'):
            query = dict(query, **{'ul':'%f,%f' % (pq.user_lat, pq.user_lon)})

        addl_settings = {'key':self._settings['api_key']}
        query = dict(query, **addl_settings)
        response_obj = self._get_json_obj(self._endpoint, query)
        returned_candidates = []  # this will be the list returned
        for r in response_obj['resourceSets'][0]['resources']:
            c = Candidate()
            c.entity = r['entityType']
            c.locator = r['geocodePoints'][0]['calculationMethod']  # ex. "Parcel"
            c.confidence = r['confidence']  # High|Medium|Low
            c.match_addr = r['name']  # ex. "1 Microsoft Way, Redmond, WA 98052"
            c.x = r['geocodePoints'][0]['coordinates'][1]  # long, ex. -122.13
            c.y = r['geocodePoints'][0]['coordinates'][0]  # lat, ex. 47.64
            c.wkid = 4326
            c.address = r['address']
            c.geoservice = self.__class__.__name__
            returned_candidates.append(c)
        return returned_candidates


class USCensus(GeocodeService):

    # set endpoint based on whether we geocode by single-line address, or with keyed components
    _endpoint = ''
    _endpoint_base = 'http://geocoding.geo.census.gov/geocoder/locations/'

    def _geocode(self, pq):
        query = {
            'format': 'json',
            'benchmark': 'Public_AR_Current'
        }

        if pq.query:
            _this_endpoint = '%s%s' % (self._endpoint_base, 'onelineaddress')
            query['address'] = pq.query
        else:
            _this_endpoint = '%s%s' % (self._endpoint_base, 'address')
            query['street'] = pq.address
            query['city'] = pq.city
            query['state'] = pq.state
            query['zip'] = pq.postal

        logger.debug('CENSUS QUERY: %s', query)
        response_obj = self._get_json_obj(_this_endpoint, query)
        logger.debug('CENSUS RESPONSE: %s', response_obj)

        returned_candidates = []  # this will be the list returned
        for r in response_obj['result']['addressMatches']:
            c = Candidate()
            c.match_addr = r['matchedAddress']
            c.x = r['coordinates']['x']
            c.y = r['coordinates']['y']
            c.geoservice = self.__class__.__name__
            # Optional address component fields.
            for in_key, out_key in [('city', 'match_city'), ('state', 'match_region'),
                                    ('zip', 'match_postal')]:
                setattr(c, out_key, r['addressComponents'].get(in_key, ''))
            setattr(c, 'match_subregion', '')  # No county from Census geocoder.
            setattr(c, 'match_country', 'USA')  # Only US results from Census geocoder
            setattr(c, 'match_streetaddr', self._street_addr_from_response(r))
            returned_candidates.append(c)
        return returned_candidates

    def _street_addr_from_response(self, match):
        """Construct a street address (no city, region, etc.) from a geocoder response.

        :param match: The match object returned by the geocoder.
        """
        # Same caveat as above regarding the ordering of these fields; the
        # documentation is not explicit about the correct ordering for
        # reconstructing a full address, but implies that this is the ordering.
        ordered_fields = ['preQualifier', 'preDirection', 'preType', 'streetName',
                          'suffixType', 'suffixDirection', 'suffixQualifier']
        result = []
        # The address components only contain a from and to address, not the
        # actual number of the address that was matched, so we need to cheat a
        # bit and extract it from the full address string. This is likely to
        # miss some edge cases (hopefully only a few since this is a US-only
        # geocoder).
        addr_num_re = re.match(r'([0-9]+)', match['matchedAddress'])
        if not addr_num_re:  # Give up
            return ''
        result.append(addr_num_re.group(0))
        for field in ordered_fields:
            result.append(match['addressComponents'].get(field, ''))
        if any(result):
            return ' '.join([s for s in result if s])  # Filter out empty strings.
        else:
            return ''


class MapQuest(GeocodeService):
    """
    Class to geocode using MapQuest licensed services.
    """
    _endpoint = 'http://www.mapquestapi.com/geocoding/v1/address'

    def _geocode(self, pq):
        def get_appended_location(location, **kwargs):
            """Add key/value pair to given dict only if value is not empty string."""
            for kw in kwargs:
                if kwargs[kw] != '':
                    location = dict(location, **{kw: kwargs[kw]})
            return location
        location = {}
        location = get_appended_location(location, street=pq.query)
        if location == {}:
            location = get_appended_location(location, street=pq.address)
        location = get_appended_location(location, city=pq.city, county=pq.subregion, state=pq.state,
                                         postalCode=pq.postal, country=pq.country)
        json_ = dict(location=location)
        json_ = json.dumps(json_)
        logger.debug('MQ json: %s', json_)
        query = dict(key=unquote(self._settings['api_key']),
                     json=json_)
        if pq.viewbox is not None:
            query = dict(query, viewbox=pq.viewbox.to_mapquest_str())
        response_obj = self._get_json_obj(self._endpoint, query)
        logger.debug('MQ RESPONSE: %s', response_obj)
        returned_candidates = [] # this will be the list returned
        for r in response_obj['results'][0]['locations']:
            c = Candidate()
            c.locator=r['geocodeQuality']
            c.confidence=r['geocodeQualityCode'] #http://www.mapquestapi.com/geocoding/geocodequality.html
            match_addr_elements = ['street', 'adminArea5', 'adminArea3',
                                   'adminArea2', 'postalCode'] # similar to ESRI
            c.match_addr = ', '.join([r[k] for k in match_addr_elements if k in r])
            c.x = r['latLng']['lng']
            c.y = r['latLng']['lat']
            c.wkid = 4326
            c.geoservice = self.__class__.__name__
            returned_candidates.append(c)
        return returned_candidates


class MapQuestSSL(MapQuest):
    _endpoint = 'https://www.mapquestapi.com/geocoding/v1/address'

class MapQuestOpen(MapQuest):
    _endpoint = 'http://open.mapquestapi.com/geocoding/v1/address'

class Nominatim(GeocodeService):
    """
    Class to geocode using `Nominatim services hosted
    by MapQuest <http://open.mapquestapi.com/nominatim/>`_.
    """
    _wkid = 4326
    _endpoint = 'http://open.mapquestapi.com/nominatim/v1/search'

    DEFAULT_ACCEPTED_ENTITIES = ['building.', 'historic.castle', 'leisure.ice_rink',
                                 'leisure.miniature_golf',
                                 'leisure.sports_centre', 'lesiure.stadium', 'leisure.track',
                                 'lesiure.water_park', 'man_made.lighthouse', 'man_made.works',
                                 'military.barracks', 'military.bunker', 'office.', 'place.house',
                                 'amenity.',  'power.generator', 'railway.station',
                                 'shop.', 'tourism.']

    DEFAULT_REJECTED_ENTITIES = ['amenity.drinking_water',
                                 'amentity.bicycle_parking', 'amentity.ev_charging',
                                 'amentity.grit_bin', 'amentity.atm',
                                 'amentity.hunting_stand', 'amentity.post_box']

    DEFAULT_PREPROCESSORS = [ReplaceRangeWithNumber()] # 766-68 Any St. -> 766 Any St.
    """Preprocessors to use with this geocoder service, in order of desired execution."""

    DEFAULT_POSTPROCESSORS = [
        AttrFilter(DEFAULT_ACCEPTED_ENTITIES, 'entity', exact_match=False),
        AttrExclude(DEFAULT_REJECTED_ENTITIES, 'entity')
    ]
    """Postprocessors to use with this geocoder service, in order of desired execution."""

    def __init__(self, preprocessors=None, postprocessors=None, settings=None):
        preprocessors = Nominatim.DEFAULT_PREPROCESSORS if preprocessors is None else preprocessors
        postprocessors = Nominatim.DEFAULT_POSTPROCESSORS if postprocessors is None else postprocessors
        GeocodeService.__init__(self, preprocessors, postprocessors, settings)

    def _geocode(self, pq):
        query = {'q':pq.query,
                 'countrycodes':pq.country, # only takes ISO-2
                 'format':'json'}

        if pq.viewbox is not None:
            query = dict(query, **{'viewbox':pq.viewbox.to_mapquest_str(), 'bounded':pq.bounded})

        response_obj = self._get_json_obj(self._endpoint, query)

        returned_candidates = [] # this will be the list returned
        for r in response_obj:
            c = Candidate()
            c.locator = 'parcel' # we don't have one but this is the closest match
            c.entity = '%s.%s' % (r['class'], r['type']) # ex.: "place.house"
            c.match_addr = r['display_name'] # ex. "Wolf Building, 340, N 12th St, Philadelphia, Philadelphia County, Pennsylvania, 19107, United States of America" #TODO: shorten w/ pieces
            c.x = float(r['lon']) # long, ex. -122.13 # cast to float in 1.3.4
            c.y = float(r['lat']) # lat, ex. 47.64 # cast to float in 1.3.4
            c.wkid = self._wkid
            c.geoservice = self.__class__.__name__
            returned_candidates.append(c)
        return returned_candidates

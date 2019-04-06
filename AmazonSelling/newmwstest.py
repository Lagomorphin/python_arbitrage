# from mwstools.parsers.products.get_my_fees_estimate import GetMyFeesEstimateResponse
from AmazonSelling.tools import get_credentials
from mwstools.mws_overrides import OverrideProducts
from decimal import Decimal

apiKeys = get_credentials({'AmazonMWS': ('marketplaceID', 'merchantID', 'accessKeyID', 'secretKey')})['AmazonMWS']

inputs = (('BT00KRV6SO', Decimal('18.66')),)

api = OverrideProducts(apiKeys['accessKeyID'], apiKeys['secretKey'], apiKeys['merchantID'])
estimate_requests = [api.gen_fees_estimate_request(apiKeys['marketplaceID'], x[0], identifier=x[0], listing_price=x[1]) for x in inputs]
try:
    response = api.get_my_fees_estimate(estimate_requests)
except Exception as err:
    print("Error with get_my_fees_estimate in the mwstools library:\ninputs:{}\n{}".format(inputs, err))

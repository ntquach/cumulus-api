import argparse
from cumulus_api import CumulusApi


def run():
    parser = argparse.ArgumentParser(description='Delete granules from Cumulus utility')
    parser.add_argument('--collectionId', type=str, required=True,
                        help='Cumulus collection ID')

    parser.add_argument('--start', type=str, required=False,
                        help='Start processing time range of granules to delete')

    parser.add_argument('--end', type=str, required=False,
                        help='End processing time range of granules to delete')

    args = parser.parse_args()

    parameters = {
        "limit": 100,
        "fields": "granuleId",
        "collectionId": args.collectionId,
        "sort_by": "beginningDateTime",
        "order": "asc"
    }
    if args.start:
        parameters["processingStartDateTime__from"] = args.start
    if args.end:
        parameters["processingStartDateTime__to"] = args.end

    cml = CumulusApi()
    granules = cml.list_granules(**parameters)
    print("Found %s matching granules" % granules['meta']['count'])

    page = 1
    while len(granules["results"]) > 0:
        for granule in granules["results"]:
            cml.remove_granule_from_cmr(granule['granuleId'])
            cml.delete_granule(granule['granuleId'])
            print("Deleted %s" % granule['granuleId'])
        page += 1
        parameters["page"] = page
        cml.refresh_token()
        granules = cml.list_granules(**parameters)


if __name__ == "__main__":
    run()

import datetime
import jwt


secret = """-----BEGIN PRIVATE KEY-----
MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQgxArW0ddxz3JdhQmC
8LPwd8SRFm56iTaBmJ0QLBphXN2gCgYIKoZIzj0DAQehRANCAARUz36LCEU8BWKX
QOwyikYT4tnk9s9wPrUriWrdZRTSIFnPz2JqgNCEYbRTn1Zvxs7JoGBPgxSixKXJ
by1WGnPa
-----END PRIVATE KEY-----"""
keyId = "88FHTYF88Y"
teamId = "NZ68UCAUPG"
alg = 'ES256'

time_now = datetime.datetime.now()
time_expired = datetime.datetime.now() + datetime.timedelta(hours=12)

headers = {
	"typ": 'JWT',
	"alg": alg,
	"kid": keyId
}

payload = {
	"iss": teamId,
	"iat": int(time_now.strftime("%s")),
	"exp": int(time_expired.strftime("%s"))
}


if __name__ == "__main__":
	"""Create an auth token"""
	token = jwt.encode(payload, secret, algorithm=alg, headers=headers).decode('UTF-8')

	print("----TOKEN----")
	print(token)

	print("----CURL----")
	print("curl -v -H 'Authorization: Bearer %s' \"https://api.music.apple.com/v1/catalog/us/songs/203709340\" " % (token))

# OUTPUT:
# curl -v -H 'Authorization: Bearer 'eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6Ijg4RkhUWUY4OFkifQ.eyJpc3MiOiJOWjY4VUNBVVBHIiwiaWF0IjoxNTA5MzI5MTA1LCJleHAiOjE1MDkzNzIzMDV9.veRYjeuf0OABXUnaa_aSnwsRHCRIZkUoE7buQKHwHv1xASY07FGTIf2dZljc0xjO6bo_xkVNohajnUNPPIyLBw'' "https://api.music.apple.com/v1/catalog/us/charts?types=songs"
# curl -v -H 'Authorization: Bearer 'eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6Ijg4RkhUWUY4OFkifQ.eyJpc3MiOiJOWjY4VUNBVVBHIiwiaWF0IjoxNTA5NzY1MzE3LCJleHAiOjE1MDk4MDg1MTd9.DS9iRoFzjIU3S4H_QtvGoxCO_Vnh6aFL8Q0Nf4M97G5_G_rlbzMbHjFFIJK4TUhezGpF7MZ9MFdCnsk2GNd2jA'' "https://api.music.apple.com/v1/catalog/us/charts?types=songs"

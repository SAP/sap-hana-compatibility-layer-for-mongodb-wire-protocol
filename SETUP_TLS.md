# Setup TLS

This guide will show how to use TLS with both mongosh and MongoDB drivers. SAP HANA compatibility layer for MongoDB Wire Protocol uses X.509 as certificate for authentification. If the purpose is to try out TLS in a developement enviroment, [mkcert](https://github.com/FiloSottile/mkcert) is a simple way to generate certificates and keys for TLS.

For mongosh and the MongoDB drivers a rootCA.pem file is needed and a file containing either the certificate or the private key. Though in some cases the latter should be a file with both the certificate and the private key. Therefore it might be necessary to create a .pem file containing both the certificate and the private key.

## Run SAP HANA compatibility layer for MongoDB Wire Protocol with TLS

This can be done easy with:

```
make run HANAConnectString=<please-insert-connect-string-here> TLS=true certFile=<path-to-certificate> keyFile=<path-to-key>
```

## TLS for mongosh

1. In docker-compose.yml add the following:

```yaml
volumes:
  # for rootCA.pem file
  - source:destination
  # for file with certificate and key
  - source:destination
```

Where source is the path to for example the folder with the rootCA.pem file and destination is the path to a folder in the container. 

2. Run the following:

```
make mongosh-tls DB=BOJER certFile=<path-to-certKeyFile-within-container> CAFile=<path-to-rootCA-within-container>
```

## TLS for MongoDB drivers

See the official documentation for each driver to connect with TLS.

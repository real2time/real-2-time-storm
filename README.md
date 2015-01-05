# Real2Time - Storm
## Version
Alpha-0.0.1

## Technology
## How to use this image

### Start a real-2-time-storm instance
```sh
$ docker run --name r2t-storm -d real2time/real-2-time-storm
```
This image includes `EXPOSE 44420` (the real-2-time-storm port), so standard container linking will make it automatically available to the linked containers.

The following environment variables are also honored for configuring your Real2Time storm instance:

- `-e NIMBUS_HOST=...` (defaults to "127.0.0.1")
- `-e R2T_STORM_LISTEN_PORT=...` (defaults to 44420)

## Supported Docker versions
This image is officially supported on Docker version 1.4.1.

Support for older versions (down to 1.0) is provided on a best-effort basis.

## User feedback
If you have any problems with or questions about this image, please contact us through a GitHub issue.

You can also reach many of the official image maintainers via the #docker-library IRC channel on Freenode.
### Issues
### Contributing

You are invited to contribute new features, fixes, or updates, large or small; we are always thrilled to receive pull requests, and do our best to process them as fast as we can.

Before you start to code, we recommend discussing your plans through a GitHub issue, especially for more ambitious contributions. This gives other contributors a chance to point you in the right direction, give you feedback on your design, and help you find out if someone else is working on the same thing.

## License
[GPLv3]

[GPLv3]:http://www.gnu.org/copyleft/gpl.html#content

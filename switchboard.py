""" switchboard.py
    Launches a fresh docker container per SSH connection  """

import sys
import time
import socket
import configparser
import glob
import random
import string
import pprint
import json
import copy
import logging
import logging.handlers

from collections import abc

import twisted
import docker

from twisted.protocols.portforward import ProxyFactory, ProxyServer, ProxyClient, ProxyClientFactory
from twisted.internet import reactor

g_logger = logging.getLogger("switchboard")

class CustomFormatter(logging.Formatter):
    """ Class formatter for logging """
    _grey = "\x1b[38;20m"
    _yellow = "\x1b[33;20m"
    _blue = "\x1b[34;20m"
    _green = "\x1b[32;20m"
    _red = "\x1b[31;20m"
    _bold_red = "\x1b[31;1m"
    _reset = "\x1b[0m"
    _format = "[%(levelname)s %(asctime)s %(funcName)s:%(lineno)d] "
    _message = "%(message)s"

    FORMATS = {
        logging.DEBUG: _blue + _format + _reset + _message,
        logging.INFO: _grey + _format + _reset + _message,
        logging.WARNING: _yellow + _format + _reset + _message,
        logging.ERROR: _red + _format + _reset + _message,
        logging.CRITICAL: _bold_red + _format + _reset + _message
    }

    def format(self, record):
        """ returns logging desired format """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class DockerPorts():
    ''' this is a global object that keeps track of the free ports
    when requested, it allocated a new docker instance and returns it '''

    CONFIG_PROFILEPREFIX = "profile:"
    CONFIG_DOCKEROPTIONSPREFIX = "dockeroptions:"

    def __init__(self):
        self.instances_by_name = {}
        self.image_params = {}

    def _get_profiles_list(self, config):
        out = []
        for n in config.sections():
            if n.startswith(self.CONFIG_PROFILEPREFIX):
                out += [n[len(self.CONFIG_PROFILEPREFIX):]]
        return out

    def _read_profile_config(self, config, profilename):
        fullprofilename = f"{self.CONFIG_PROFILEPREFIX}{profilename}"
        innerport = self._parse_int(config[fullprofilename]["innerport"])
        checkupport = innerport
        limit = 0
        reuse = False

        if "checkupport" in config[fullprofilename]:
            checkupport = self._parse_int(config[fullprofilename]["checkupport"])
        if "limit" in config[fullprofilename]:
            limit = self._parse_int(config[fullprofilename]["limit"])
        if "reuse" in config[fullprofilename]:
            reuse = self._parse_truthy(config[fullprofilename]["reuse"])

        return {
            "outerport": int(config[fullprofilename]["outerport"]),
            "innerport": innerport,
            "containername": config[fullprofilename]["container"],
            "checkupport": checkupport,
            "limit": limit,
            "reuse": reuse,
            "dockeroptions": self._get_docker_options(
                config, profilename, innerport, checkupport)
        }

    def _add_docker_options_from_config_section(self, config, sectionname, base):
        """ read options from config file """
        def update(d, u):
            for k, v in u.items():
                if isinstance(v, abc.Mapping):
                    r = update(d.get(k, {}), v)
                    d[k] = r
                else:
                    d[k] = u[k]
            return d

        def guessvalue(v):
            """ we may need to read json values """
            if v in ["True", "False"] or all(c in string.digits for c in v) or \
                v.startswith("[") or v.startswith("{"):
                return json.loads(v)
            return v

        # if sectionname doesn't exist, return base
        # otherwise, read keywords and values, add them to base
        if sectionname in config.sections():
            newvals = dict(config[sectionname])
            fixedvals = {}
            for (k,v) in newvals.items():
                fixedvals[k] = guessvalue(v)
            base = update(base, fixedvals)

        return base

    def _get_docker_options(self, config, profilename, innerport, checkupport):
        """ Get docker options form config """
        out = {}
        out = self._add_docker_options_from_config_section(
            config, "dockeroptions", {})
        out = self._add_docker_options_from_config_section(
            config, f"{self.CONFIG_DOCKEROPTIONSPREFIX}{profilename}", out)

        out["detach"] = True
        if "ports" not in out:
            out["ports"] = {}
        out["ports"][innerport] = None
        out["ports"][checkupport] = None
        # cannot use detach and remove together
        # See https://github.com/docker/docker-py/issues/1477
        #out["remove"] = True
        #out["auto_remove"] = True
        return out

    def read_config(self, fn):
        """ read the configfile. """
        config = configparser.ConfigParser()
        g_logger.debug("Reading configfile from %s", fn)
        config.read(fn)

        # set log file
        if "global" in config.sections() and "logfile" in config["global"]:
            if "global" in config.sections() and "rotatelogfileat" in config["global"]:
                handler = logging.handlers.TimedRotatingFileHandler(
                    config["global"]["logfile"],
                    when=config["global"]["rotatelogfileat"])
            else:
                handler = logging.FileHandler(config["global"]["logfile"])
            handler.setFormatter(CustomFormatter())
            g_logger.addHandler(handler)

        # Log to the screen, too
        handler = logging.StreamHandler()
        handler.setFormatter(CustomFormatter())
        g_logger.addHandler(handler)

        # set log level
        if "global" in config.sections() and "loglevel" in config["global"]:
            # global logger
            g_logger.setLevel(logging.getLevelName(config["global"]["loglevel"]))

        # if there is a configdir directory, reread everything
        if "global" in config.sections() and "splitconfigfiles" in config["global"]:
            fnlist = [fn] + [f for f in glob.glob(config["global"]["splitconfigfiles"])]
            g_logger.debug("Detected configdir directive. Reading configfiles from %s", fnlist)
            config = configparser.ConfigParser()
            config.read(fnlist)

        if len(self._get_profiles_list(config)) == 0:
            g_logger.error("invalid configfile. No docker images")
            sys.exit(1)

        for profilename in self._get_profiles_list(config):
            conf = self._read_profile_config(config, profilename)
            g_logger.debug("Read config for profile %s", profilename)

            self.register_proxy(profilename, conf)

        params = {}
        for key, value in self.image_params.items():
            params[key] = value["outerport"]
        return params

    def _parse_int(self, x):
        """ converts to integer """
        return int(x)

    def _parse_truthy(self, x):
        """ converts to boolean """
        if x.lower() in ["0", "false", "no"]:
            return False
        if x.lower() in ["1", "true", "yes"]:
            return True

        raise f"Unknown truthy value {x}"

    def register_proxy(self, profilename, conf):
        """ store copy of config for this profile """
        self.image_params[profilename] = copy.deepcopy(conf)

    def create(self, profilename):
        """ create docker instance """
        containername = self.image_params[profilename]["containername"]
        dockeroptions = self.image_params[profilename]["dockeroptions"]
        imagelimit = self.image_params[profilename]["limit"]
        reuse = self.image_params[profilename]["reuse"]
        innerport = self.image_params[profilename]["innerport"]
        checkupport = self.image_params[profilename]["checkupport"]

        icount = 0
        if profilename in self.instances_by_name:
            icount = len(self.instances_by_name[profilename])

        if icount >= imagelimit > 0:
            g_logger.warning(
                "Reached max count of %d (currently %d) for image %s",
                imagelimit, icount, profilename)
            return None

        instance = None

        if reuse and icount > 0:
            g_logger.debug("Reusing existing instance for image %s", profilename)
            instance = self.instances_by_name[profilename][0]
        else:
            instance = DockerInstance(
                profilename, containername, innerport, checkupport, dockeroptions)
            instance.start()

        if profilename not in self.instances_by_name:
            self.instances_by_name[profilename] = []

        # in case of reuse, the list will have duplicates
        self.instances_by_name[profilename] += [instance]

        return instance

    def destroy(self, instance):
        """ destroy docker instance """
        profilename = instance.get_profile_name()
        reuse = self.image_params[profilename]["reuse"]

        # in case of reuse, the list will have duplicates, but remove() does not care
        self.instances_by_name[profilename].remove(instance)

        # stop the instance if there is no reuse, or if this is the last instance for a reused image
        if not reuse or len(self.instances_by_name[profilename]) == 0:
            instance.stop()


class DockerInstance():
    """ this class represents a single docker instance listening on a certain middleport.
    The middleport is managed by the DockerPorts global object
    After the docker container is started, we wait until the middleport becomes reachable
    before returning """

    def __init__(self, profilename, containername, innerport, checkupport, dockeroptions):
        self._profilename = profilename
        self._containername = containername
        self._dockeroptions = dockeroptions
        self._innerport = innerport
        self._checkupport = checkupport
        self._instance = None

    def get_docker_options(self):
        """ Return docker options """
        return self._dockeroptions

    def get_container_name(self):
        """ return container's name """
        return self._containername

    def get_mapped_port(self, inp):
        """ return container mapped port """
        try:
            return int(
                self._instance.attrs["NetworkSettings"]["Ports"][f"{inp}/tcp"][0]["HostPort"])
        except Exception as e:
            g_logger.warning(
                "Failed to get port information for port %d from %d: %s",
                inp, self.get_instance_id(), e)
        return None

    def get_middle_port(self):
        """ returns inner port """
        return self.get_mapped_port(self._innerport)

    def get_middle_checkup_port(self):
        """ gets checkup port """
        return self.get_mapped_port(self._checkupport)

    def get_profile_name(self):
        """ get profile name """
        return self._profilename

    def get_instance_id(self):
        """ get instance id """
        try:
            return self._instance.id
        except Exception as e:
            g_logger.warning("Failed to get instance id: %s", e)
        return "None"

    def start(self):
        """ Start this instance """

        # get docker client
        client = docker.from_env()

        # start instance
        try:
            g_logger.debug("Starting instance %s of container %s with dockeroptions %s",
                self.get_profile_name(),
                self.get_container_name(),
                pprint.pformat(self.get_docker_options()))

            clientres = client.containers.run(
                self.get_container_name(), **self.get_docker_options())

            self._instance = client.containers.get(clientres.id)

            g_logger.debug("Done starting instance %s of container %s",
                self.get_profile_name(), self.get_container_name())
        except Exception as e:
            g_logger.debug("Failed to start instance %s of container %s: %s",
                self.get_profile_name(), self.get_container_name(), e)
            self.stop()
            return False

        # wait until container's checkupport is available
        g_logger.debug("Started instance on middleport %s with ID %s",
            self.get_middle_port(), self.get_instance_id())

        if self._wait_for_open_port(self.get_middle_checkup_port()):
            g_logger.debug("Started instance on middleport %d with ID %s has open port %d",
                self.get_middle_port(), self.get_instance_id(), self.get_middle_checkup_port())
            return True

        g_logger.debug("Started instance on middleport %d with ID %s has closed port %d",
            self.get_middle_port(), self.get_instance_id(), self.get_middle_checkup_port())
        self.stop()
        return False

    def stop(self):
        """ stop docker instance """
        mp = self.get_middle_port()
        cid = self.get_instance_id()
        g_logger.debug("Killing and removing %s (middleport %d)", cid, mp)

        try:
            self._instance.remove(force=True)
        except Exception as e:
            g_logger.warning("Failed to remove instance for middleport %d, id %s: %s",
                mp, cid, e)
            return False
        return True

    def _is_port_open(self, port, readtimeout=0.1):
        s = socket.socket()
        ret = False
        g_logger.debug("Checking whether port %d is open...", port)

        if port is None:
            time.sleep(readtimeout)
        else:
            try:
                s.connect(("0.0.0.0", port))
                # just connecting is not enough, we should try to read and get at least 1 byte
                # back since the daemon in the container might not have started accepting
                # connections yet, while docker-proxy does
                s.settimeout(readtimeout)
                data = s.recv(1)
                ret = len(data) > 0
            except socket.error:
                ret = False

        g_logger.debug("result = %s", ret)
        s.close()
        return ret

    def _wait_for_open_port(self, port, timeout=5, step=0.1):
        """ waits until instance is running """
        started = time.time()

        while started + timeout >= time.time():
            if self._is_port_open(port):
                return True
            time.sleep(step)
        return False


class LoggingProxyClient(ProxyClient):
    """ Logging proxy client """
    def dataReceived(self, data):
        payloadlen = len(data)
        self.factory.server.up_bytes += payloadlen
        self.peer.transport.write(data)

class LoggingProxyClientFactory(ProxyClientFactory):
    """ Logging proxy client factory """
    protocol = LoggingProxyClient

class DockerProxyServer(ProxyServer):
    """ Docker proxy server """
    clientProtocolFactory = LoggingProxyClientFactory
    reactor = None

    def __init__(self):
        super().__init__()
        self.down_bytes = 0
        self.up_bytes = 0
        self.session_id = "".join([random.choice(string.ascii_letters) for _ in range(16)])
        self.session_start = time.time()
        self.docker_instance = None

    def connectionMade(self):
        """ This is a reimplementation, except that we want to specify host and port... """

        # Don't read anything from the connecting client until we have
        # somewhere to send it to.
        self.transport.pauseProducing()

        client = self.clientProtocolFactory()
        client.setServer(self)

        if self.reactor is None:
            self.reactor = reactor

        self.docker_instance = g_docker_ports.create(self.factory.profilename)

        if self.docker_instance is None:
            self.transport.write(
                bytearray("Maximum connection-count reached. Try again later.\r\n", "utf-8"))
            self.transport.loseConnection()
        else:
            g_logger.info("[Session %s] Incoming connection for image %s from %s at %s",
                self.session_id,
                self.docker_instance.get_profile_name(),
                self.transport.getPeer(),
                self.session_start)

            self.reactor.connectTCP("0.0.0.0", self.docker_instance.get_middle_port(), client)

    def connectionLost(self, reason):
        profilename = "<none>"
        if self.docker_instance is not None:
            g_docker_ports.destroy(self.docker_instance)
            profilename = self.docker_instance.get_profile_name()

        self.docker_instance = None
        super().connectionLost(reason)
        timenow = time.time()
        g_logger.info(
            "[Session %s] server disconnected session for image %s from %s \
                (start=%s, end=%s, duration=%s, upBytes=%d, downBytes=%d, totalBytes=%d)",
                self.session_id, profilename, self.transport.getPeer(),
                self.session_start, timenow, timenow-self.session_start,
                self.up_bytes, self.down_bytes, self.up_bytes + self.down_bytes)

    def dataReceived(self, data):
        payloadlen = len(data)
        self.down_bytes += payloadlen
        self.peer.transport.write(data)


class DockerProxyFactory(ProxyFactory):
    protocol = DockerProxyServer

    def __init__(self, profilename):
        self.profilename = profilename


def main():
    """ main function. Program starts here """

    ports_and_names = g_docker_ports.read_config(
        sys.argv[1] if len(sys.argv) > 1 else 'switchboard.conf')

    try:
        for (name, outerport) in ports_and_names.items():
            g_logger.debug("Listening on port %d", outerport)
            reactor.listenTCP(
                outerport,
                DockerProxyFactory(name),
                interface=sys.argv[2] if len(sys.argv) > 2 else '')
        reactor.run()
    except twisted.internet.error.CannotListenError as err:
        print(err)

g_docker_ports = DockerPorts()

if __name__ == "__main__":
    main()

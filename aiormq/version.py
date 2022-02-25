author_info = (("Dmitry Orlov", "me@mosquito.su"),)

package_info = "Pure python AMQP asynchronous client library"
package_license = "Apache Software License"

team_email = "me@mosquito.su"

version_info = (6, 2, 3)

__author__ = ", ".join("{} <{}>".format(*info) for info in author_info)
__version__ = ".".join(map(str, version_info))

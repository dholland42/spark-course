# spark-course
This repo follows the udemy course _Taming Big Data with Apache Spark and Python - Hands On!_

This exercise was performed as part of
[athenahealth](https://www.athenahealth.com/)'s Data
Science team. If you are a member of that team and are also
going through the course, please feel free to reach out! If you
are going through the course outside of athena and would like
to reach out you can contact me at
[danielholland414@gmail.com](mailto:danielholland414@gmail.com?subject=spark-course)
and I will do my best to help out. I certainly don't have all the
answers (as I'm no expert and am learning this at the same time
as you!), but I really enjoy learning as a group so please don't
hesitate to drop me a line.

## Prerequisites
To use the setup here, you will need to have docker installed.
Check out the web page for instructions on
[how to install docker](https://docs.docker.com/get-docker/)
for your system.

## Set Up
This repo is here to help make installation and setup of spark
easier so you can get down to business learning how to crush
data. It is particularly useful if you are using Visual Studio
Code. If you prefer a different IDE, do not fear! You can still
use the dockerized environment defined here.

### VSCode
If you're using vscode, make sure to have the "remote containers"
extension from Microsoft installed. Then, simply clone the repo
and open it in vscode. You should get an alert that there is a dev
container defined and be prompted to reopen inside a container. If
not, open the command seach (Ctrl/Command + Shift + P) and search
for the command "Remote-Containers: Reopen in Container". The
first time you do this, it will take some time as the container
will need to be built.

### Other IDE
If you prefer pycharm, or you're a legendary dev who will never
let go of Vim, you can simply build the dockerized environment
yourself and use whatever you want to edit the code. Your IDE may
even have the ability to edit code remotely in a container. To
build the docker image, you will want to run the following command
from the root directory of this repo:

    docker build -t spark-course --build-arg DEV_USER=$USER .

This will build the image and add the current user to the system.
This way, there (hopefully) won't be any permission issues when
editing code that is mounted from a local volume. You should then
be able to run the image with the command

    docker run -it -u $USER spark-course

NOTE: This will just run a container that has python and spark all
set up. It is up to you to figure out where you want your code to
live and how to run that code in the container. If you write your
code in the container in the first place, then no problem!
Otherwise, you're going to want to mount the location where you
keep your solutions inside the container using the `-v` flag in
your `docker run` command.

## Solutions
This main branch is intentionally left blank with no code. If
you're interested, I've included my solutions on the
[solutions](https://github.com/dholland42/spark-course/tree/solutions)
branch.
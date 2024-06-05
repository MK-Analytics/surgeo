Surgeo
==============

.. image:: static/logo.gif

The documentation for Surgeo may be found here: `<https://surgeo.readthedocs.io/en/master/>`_

Contributors
------------
* `Ryan Willett <https://github.com/rtwillett>`_
* `John Willis <https://github.com/likealostcause>`_
* `MK Analytics <https://github.com/MK-Analytics>`_
* `Adam Weeden <https://github.com/TheCleric>`_
* `Algorex Health <https://github.com/AlgorexHealth>`_
* `Theo Naunheim <https://github.com/theonaunheim>`_

Overview
--------

**Surgeo** is a module that contains a variety of open source demographic
tools that allow you to construct race probabilities from more commonly
available information such as location, first name, and last name
information. This imputed race data is often used in the public health
and fair lending contexts when race information is not otherwise
available.

Specifically Surgeo contains the following models:

* **Bayesian Improved First Name Surname Geocode (BIFSG)**: an adaptation
  of an algorithm created by Ioan Voicu that uses forename, surname, and
  location information to obtain probable races
* **Bayesian Improved Surname Geocode (BISG)**: an adaptation of an algorithm
  created by Mark Elliot and popularized by the Consumer Financial Protection
  Bureau (CFPB) that uses surname and location to obtain probable races
* **Forename**: a helper model to pull race data based on first name
* **Surname**: a helper model to pull race data based on last name
* **Geocode**: a helper model to pull race data based on location

Please see the ReadTheDocs link above for additional information on the
data sources used and the implementations themselves.

Installation
------------

To install surgeo as an executable, please see the installer below.

To install as a Python module using SSH credentials:

.. code-block::

    $ pip install git+ssh://git@github.com:MK-Analytics/surgeo.git

Usage
-----

Surgeo can currently be used a Python module. Details
follow.

As a Module
~~~~~~~~~~~

Surgeo is best used as a module.

.. code-block:: python

    import pandas as pd
    import surgeo

    # Instatiate your model
    fsg = surgeo.BIFSGModel()

    # Create pd.Series objects to analze (or load them)
    first_names = pd.Series(['HECTOR', 'PHILLIP', 'JANICE'])
    surnames = pd.Series(['DIAZ', 'JOHNSON', 'WASHINGTON'])
    zctas = pd.Series(['65201', '63144', '63110'])

    # Get results using the get_probabilities() function
    fsg_results = fsg.get_probabilities(first_names, surnames, zctas)

    # Show Surgeo BIFSG results
    fsg_results

.. image:: static/model_results.gif
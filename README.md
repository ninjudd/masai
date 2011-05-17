# Masai

Masai is a very simple interface to a number of key-value stores. It leverages Clojure's protocols and datatypes to make pluggable backends easy. Masai is meant to be a uniform and simple interface to pluggable backends, and not a comprehensive interface to all of them.

Currently supported by Masai are Tokyo Cabinent and Redis. Memcached support is in the works.

# Getting It

You can get Masai from [clojars](http://clojars.org) with [cake](https://github.com/ninjudd/cake). Just add it as a dependency to your project like so:

    [masai "0.6.0"]

Masai doesn't download the necessary dependencies for all of its backends. Our reasoning is that it would be wasteful to require users to download dependencies for all of the backends when they only need one of them. Therefore, you'll need to add dependencies for whatever backend you plan to use. You'll want to look in Masai's project.clj for `:dev-dependencies`. Those are the dependencies that we test Masai with for each backend. Use whatever dependencies you need from there.

# Usage

First of all, decide what backend you want to use. Install whatever is necessary and get that running. After that, you'll want to `use` whatever backend you're using and Masai's protocol namespace, `masai.db`. For example, if you decided to use the redis backend:

    (ns my.ns
      (:use masai.db masai.redis))

Now you need to create an instance of DB to work with. Every backend has a `make` function for doing this.

    (def db (make))

This will create a default instance of it for you. This assumes that your installation of whatever key-value store the backend corresponds to is default as well. If you're running redis on a non-default port, this wont work. You can specify the port by passing `:port newport` to `make`.

Some backends, such as tokyo, require that you run the `open` function before the database will be usable. In those cases, do this:

    (open db)

Now you can work with your db. You can add stuff:

    (add! db "foo" (.getBytes "bar"))

Get stuff:

    (get "db" "foo")

And even delete stuff:

    (delete! db "foo")

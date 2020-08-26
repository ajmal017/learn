# Overall idea

## Step # 1 Underlyings
====================

Build underlying contracts
* [x] Set the market
* [x] Imports, connections, log and timer
* [x] Get underlying symbols and lots
* [x] Qualify the underlying symbols

Qualify all underlyings
...pickle all successful contracts

## Step # 2: Base
==============

For each qualified underlying get ohlc, price, market data and chains
...pickle after each successful 'done'

## Step # 3: Options
=================

Make a todo list for each qualified underlying, if it does not exist
Assemble options based on the todo list's chains
Qualify the assembled options per underlying symbol
...pickle after each successful 'done'
...remove it from the todo list

## Step # 4: Prices and Margins
============================

Make a todo list for each qualified option by underlying
Get the price and margin for the qualified option by underlying
...pickle after each successful 'done'
...remove it from the todo list

## Errors
======

a) Connection error
...if there is a connection error cancel all `pending` tasks gracefully


b) Timeout error
...if there is a timeout error on a particular contract or underlying, log it separately

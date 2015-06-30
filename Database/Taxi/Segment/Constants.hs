{-|
  Module      : Database.Taxi.Segment.Constants
  Description : Constants
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

module Database.Taxi.Segment.Constants where

tempDir :: FilePath
tempDir = "tmp"

dataExtension :: String
dataExtension = "dat"

indexExtension :: String
indexExtension = "hint"

deletedDocsFile :: String
deletedDocsFile = "del"

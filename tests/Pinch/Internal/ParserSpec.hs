{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NegativeLiterals #-}
module Pinch.Internal.ParserSpec (spec) where

import Data.Either           (isLeft)
import Data.Word             (Word8)
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck

import qualified Data.ByteString as B

import Pinch.Arbitrary

import qualified Pinch.Internal.Parser as P


parse :: [Word8] -> P.Parser a -> Either String a
parse s p = P.runParser p (B.pack s)


parseCases
    :: (Show a, Eq a)
    => P.Parser a -> [([Word8], a)] -> Expectation
parseCases parser = mapM_ . uncurry $ \bytes expected ->
    (bytes `parse` parser) `shouldBe` Right expected


spec :: Spec
spec = describe "Parser" $ do

    it "can succeed" $
        P.runParser (return 42) "" `shouldBe` Right (42 :: Int)

    it "can fail" $
        (P.runParser (fail "great sadness") "" :: Either String ())
            `shouldBe` Left "great sadness"

    it "can parse 8-bit integers." $
        parseCases P.int8
            [ ([0x01], 1)
            , ([0x05], 5)
            , ([0x7f], 127)
            , ([0xff], -1)
            , ([0x80], -128)
            ]

    it "can parse 16-bit integers." $
        parseCases P.int16
            [ ([0x00, 0x01], 1)
            , ([0x00, 0xff], 255)
            , ([0x01, 0x00], 256)
            , ([0x01, 0x01], 257)
            , ([0x7f, 0xff], 32767)
            , ([0xff, 0xff], -1)
            , ([0xff, 0xfe], -2)
            , ([0xff, 0x00], -256)
            , ([0xff, 0x01], -255)
            , ([0x80, 0x00], -32768)
            ]

    it "can parse 32-bit integers." $
        parseCases P.int32
            [ ([0x00, 0x00, 0x00, 0x01], 1)
            , ([0x00, 0x00, 0x00, 0xff], 255)
            , ([0x00, 0x00, 0xff, 0xff], 65535)
            , ([0x00, 0xff, 0xff, 0xff], 16777215)
            , ([0x7f, 0xff, 0xff, 0xff], 2147483647)
            , ([0xff, 0xff, 0xff, 0xff], -1)
            , ([0xff, 0xff, 0xff, 0x00], -256)
            , ([0xff, 0xff, 0x00, 0x00], -65536)
            , ([0xff, 0x00, 0x00, 0x00], -16777216)
            , ([0x80, 0x00, 0x00, 0x00], -2147483648)
            ]

    it "can parse 64-bit integers." $
        parseCases P.int64
            [ ([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01], 1)
            , ([0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff], 4294967295)
            , ([0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff], 1099511627775)
            , ([0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 281474976710655)
            , ([0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 72057594037927935)
            , ([0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 9223372036854775807)
            , ([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], -1)
            , ([0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00], -4294967296)
            , ([0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00], -1099511627776)
            , ([0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], -281474976710656)
            , ([0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], -72057594037927936)
            , ([0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], -9223372036854775808)
            ]

    -- TODO similar test cases for double

    describe "take" $ do

        prop "returns the requested number of bytes" $
          \n (SomeByteString bs) ->
            B.length bs >= n ==>
              (B.length <$> P.runParser (P.take n) bs) === Right n

        prop "fails when input is too short" $
          \n (SomeByteString bs) ->
            B.length bs < n ==> isLeft (P.runParser (P.take n) bs)

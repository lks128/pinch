{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
-- |
-- Module      :  Pinch.Pinchable
-- Copyright   :  (c) Abhinav Gupta 2015
-- License     :  BSD3
--
-- Maintainer  :  Abhinav Gupta <mail@abhinavg.net>
-- Stability   :  experimental
--
-- Types that can be serialized into Thrift payloads implement the 'Pinchable'
-- typeclass.
--
module Pinch.Pinchable
    ( Pinchable(..)
    , (.=)
    , (?=)
    , struct
    , FieldPair
    , (.:)
    , (.:?)
    ) where


#if __GLASGOW_HASKELL__ < 709
import Control.Applicative
#endif

import Data.ByteString     (ByteString)
import Data.Hashable       (Hashable)
import Data.HashMap.Strict (HashMap)
import Data.Int            (Int16, Int32, Int64, Int8)
import Data.Text           (Text)
import Data.Typeable       ((:~:) (..), Typeable, eqT)
import Data.Vector         (Vector)

import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet        as HS
import qualified Data.Map.Strict     as M
import qualified Data.Set            as S
import qualified Data.Text.Encoding  as TE
import qualified Data.Vector         as V

import Pinch.Internal.Pinchable
import Pinch.Internal.TType
import Pinch.Internal.Value


-- TODO helper to pinch/unpinch enums
-- TODO helper to pinch/unpinch unions


-- | Helper to 'unpinch' values by matching TTypes.
checkedUnpinch
    :: forall a b. (Pinchable a, Typeable b)
    => Value b -> Either String a
checkedUnpinch = case eqT of
    Nothing -> const $ Left "Type mismatch"
    Just (Refl :: Tag a :~: b) -> unpinch

-- | Helper to 'pinch' maps.
pinchMap
    :: forall k v kval vval m.
        ( Pinchable k
        , Pinchable v
        , kval ~ Value (Tag k)
        , vval ~ Value (Tag v)
        )
    => ((k -> v -> HashMap kval vval -> HashMap kval vval)
           -> HashMap kval vval -> m -> HashMap kval vval)
          -- ^ @foldrWithKey@
    -> m  -- ^ map that implements @foldrWithKey@
    -> Value TMap
pinchMap folder = VMap . folder go HM.empty
  where
    go k v = HM.insert (pinch k) (pinch v)


instance IsTType a => Pinchable (Value a) where
    type Tag (Value a) = a
    pinch = id
    unpinch = Right

instance Pinchable ByteString where
    type Tag ByteString = TBinary
    pinch = VBinary
    unpinch (VBinary b) = Right b
    unpinch x = Left $ "Failed to read binary. Got " ++ show x

instance Pinchable Text where
    type Tag Text = TBinary
    pinch = VBinary . TE.encodeUtf8
    unpinch (VBinary b) = Right . TE.decodeUtf8 $ b
    unpinch x = Left $ "Failed to read string. Got " ++ show x

instance Pinchable Bool where
    type Tag Bool = TBool
    pinch = VBool
    unpinch (VBool x) = Right x
    unpinch x = Left $ "Failed to read boolean. Got " ++ show x

instance Pinchable Int8 where
    type Tag Int8 = TByte
    pinch = VByte
    unpinch (VByte x) = Right x
    unpinch x = Left $ "Failed to read byte. Got " ++ show x

instance Pinchable Double where
    type Tag Double = TDouble
    pinch = VDouble
    unpinch (VDouble x) = Right x
    unpinch x = Left $ "Failed to read double. Got " ++ show x

instance Pinchable Int16 where
    type Tag Int16 = TInt16
    pinch = VInt16
    unpinch (VInt16 x) = Right x
    unpinch x = Left $ "Failed to read i16. Got " ++ show x

instance Pinchable Int32 where
    type Tag Int32 = TInt32
    pinch = VInt32
    unpinch (VInt32 x) = Right x
    unpinch x = Left $ "Failed to read i32. Got " ++ show x

instance Pinchable Int64 where
    type Tag Int64 = TInt64
    pinch = VInt64
    unpinch (VInt64 x) = Right x
    unpinch x = Left $ "Failed to read i64. Got " ++ show x

instance Pinchable a => Pinchable (Vector a) where
    type Tag (Vector a) = TList
    pinch = VList . V.map pinch
    unpinch (VList xs) = V.mapM checkedUnpinch xs
    unpinch x = Left $ "Failed to read list. Got " ++ show x

instance Pinchable a => Pinchable [a] where
    type Tag [a] = TList
    pinch = VList . V.fromList . map pinch
    unpinch (VList xs) = mapM checkedUnpinch $ V.toList xs
    unpinch x = Left $ "Failed to read list. Got " ++ show x

instance
  ( Eq k
  , Hashable k
  , Pinchable k
  , Pinchable v
  ) => Pinchable (HM.HashMap k v) where
    type Tag (HM.HashMap k v) = TMap
    pinch = pinchMap HM.foldrWithKey

    unpinch (VMap xs) =
        fmap HM.fromList . mapM go $ HM.toList xs
      where go (k, v) = (,) <$> checkedUnpinch k <*> checkedUnpinch v
    unpinch x = Left $ "Failed to read map. Got " ++ show x

instance (Ord k, Pinchable k, Pinchable v) => Pinchable (M.Map k v) where
    type Tag (M.Map k v) = TMap
    pinch = pinchMap M.foldrWithKey

    unpinch (VMap xs) =
        fmap M.fromList . mapM go $ HM.toList xs
      where go (k, v) = (,) <$> checkedUnpinch k <*> checkedUnpinch v
    unpinch x = Left $ "Failed to read map. Got " ++ show x

instance (Eq a, Hashable a, Pinchable a) => Pinchable (HS.HashSet a) where
    type Tag (HS.HashSet a) = TSet
    pinch = VSet . HS.map pinch
    unpinch (VSet xs) =
        fmap HS.fromList . mapM checkedUnpinch $ HS.toList xs
    unpinch x = Left $ "Failed to read set. Got " ++ show x

instance (Ord a, Pinchable a) => Pinchable (S.Set a) where
    type Tag (S.Set a) = TSet
    pinch = VSet . S.foldr (HS.insert . pinch) HS.empty
    unpinch (VSet xs) =
        fmap S.fromList . mapM checkedUnpinch $ HS.toList xs
    unpinch x = Left $ "Failed to read set. Got " ++ show x

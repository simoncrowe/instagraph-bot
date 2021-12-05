graph [
  directed 1
  node [
    id 1
    label "1"
    identifier "1"
    username "one"
    fullName "User One"
  ]
  node [
    id 2
    label "2"
    identifier "2"
    username "two"
    fullName "User Two"
  ]
  node [
    id 3
    label "3"
    identifier "3"
    username "three"
    fullName "User Three"
  ]
  node [
    id 4
    label "4"
    identifier "4"
    username "four"
    fullName "User Four"
  ]
  edge [
    source 1
    target 2
  ]
  edge [
    source 2
    target 1
  ]
  edge [
    source 2
    target 3
  ]
  edge [
    source 3
    target 1
  ]
  edge [
    source 3
    target 4
  ]
]

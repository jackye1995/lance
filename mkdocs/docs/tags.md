# Manage Tags

Lance, much like Git, employs the
`LanceDataset.tags <lance.LanceDataset.tags>`{.interpreted-text
role="py:attr"} property to label specific versions within a dataset\'s
history.

`Tags <lance.dataset.Tags>`{.interpreted-text role="py:class"} are
particularly useful for tracking the evolution of datasets, especially
in machine learning workflows where datasets are frequently updated. For
example, you can `create <lance.dataset.Tags.create>`{.interpreted-text
role="py:meth"}, `update <lance.dataset.Tags.update>`{.interpreted-text
role="meth"}, and `delete <lance.dataset.Tags.delete>`{.interpreted-text
role="meth"} or `list <lance.dataset.Tags.list>`{.interpreted-text
role="py:meth"} tags.

!!! note

Creating or deleting tags does not generate new dataset versions. Tags
exist as auxiliary metadata stored in a separate directory.

!!! note

Tagged versions are exempted from the
`LanceDataset.cleanup_old_versions() <lance.LanceDataset.cleanup_old_versions>`{.interpreted-text
role="py:meth"} process.

To remove a version that has been tagged, you must first
`LanceDataset.tags.delete() <lance.dataset.Tags.delete>`{.interpreted-text
role="py:meth"} the associated tag.

